//! Branch listing (`git.getBranches`).
//!
//! Ports the TS `git.getBranches` handler: local branches, optional
//! `origin/*` remote branches, current branch, and the default branch
//! (`origin/HEAD`, falling back to `master`/`main`). The "known repo"
//! authorization check is wire policy and lives in `intent-services`.

use std::path::Path;

use git2::{BranchType, Repository, Status, StatusOptions};
use intent_core::{GitBranchStatus, GitBranches, Result};

use crate::map_git_err;
use crate::status::current_branch;

/// List branches for the repository at `repo_path`.
pub fn get_branches(repo_path: &Path, include_remote: bool) -> Result<GitBranches> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let current = current_branch(&repo);
    let local = local_branches(&repo)?;
    let default_branch = default_branch(&repo, &local);
    let remote_branches = if include_remote {
        remote_branches(&repo, &local, &default_branch)?
    } else {
        Vec::new()
    };
    let branches = sort_local(local, &default_branch, &current);
    Ok(GitBranches {
        branches,
        remote_branches,
        current_branch: current,
        default_branch,
    })
}

/// Branch status for `branch_name` in the repository at `repo_path`: ahead/behind
/// vs `refs/remotes/origin/<branch_name>` (`(0, 0)` when there is no upstream,
/// mirroring the TS `git rev-list ... || 0\t0` fallback), the worktree's
/// currently-checked-out branch (with `is_current_branch` derived against the
/// queried name), and whether the working tree has any uncommitted changes
/// (staged, unstaged, or untracked — matching the legacy
/// `git status --porcelain` semantics). Local-only: no fetch is performed.
pub fn branch_status(repo_path: &Path, branch_name: &str) -> Result<GitBranchStatus> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let current = current_branch(&repo);
    let (ahead, behind) = ahead_behind_vs_origin(&repo, branch_name);
    let has_uncommitted_changes = has_any_changes(&repo)?;
    Ok(GitBranchStatus {
        branch: branch_name.to_string(),
        is_current_branch: current == branch_name,
        current_branch: current,
        ahead,
        behind,
        has_uncommitted_changes,
    })
}

/// Ahead/behind of HEAD vs `refs/remotes/origin/<branch_name>`. Returns
/// `(0, 0)` when HEAD or the upstream ref is unresolvable (no upstream
/// configured, unborn HEAD, etc.) — mirrors the TS `0\t0` fallback in
/// `git rev-list --left-right --count HEAD...origin/<branch>`.
fn ahead_behind_vs_origin(repo: &Repository, branch_name: &str) -> (i64, i64) {
    if branch_name.is_empty() {
        return (0, 0);
    }
    let Some(local) = repo.head().ok().and_then(|h| h.target()) else {
        return (0, 0);
    };
    let upstream_ref = format!("refs/remotes/origin/{branch_name}");
    let Some(upstream) = repo
        .find_reference(&upstream_ref)
        .ok()
        .and_then(|r| r.target())
    else {
        return (0, 0);
    };
    match repo.graph_ahead_behind(local, upstream) {
        Ok((a, b)) => (a as i64, b as i64),
        Err(_) => (0, 0),
    }
}

/// Whether the working tree has any uncommitted changes (staged, unstaged, or
/// untracked), mirroring the legacy `git status --porcelain` "any output ⇒
/// dirty" check. Ignored files are excluded (parity with the porcelain default).
fn has_any_changes(repo: &Repository) -> Result<bool> {
    let mut opts = StatusOptions::new();
    opts.include_untracked(true)
        .recurse_untracked_dirs(true)
        .include_ignored(false)
        .include_unmodified(false);
    let statuses = repo.statuses(Some(&mut opts)).map_err(map_git_err)?;
    Ok(statuses
        .iter()
        .any(|e| !e.status().contains(Status::IGNORED) && !e.status().is_empty()))
}

fn local_branches(repo: &Repository) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for branch in repo
        .branches(Some(BranchType::Local))
        .map_err(map_git_err)?
    {
        let (branch, _) = branch.map_err(map_git_err)?;
        if let Some(name) = branch.name().map_err(map_git_err)? {
            out.push(name.to_string());
        }
    }
    Ok(out)
}

/// Resolve the default branch: `origin/HEAD`'s symbolic target, else `master`
/// when it exists locally, else `main` (matching the TS fallback chain).
fn default_branch(repo: &Repository, local: &[String]) -> String {
    if let Ok(reference) = repo.find_reference("refs/remotes/origin/HEAD") {
        if let Ok(Some(target)) = reference.symbolic_target() {
            if let Some(name) = target.strip_prefix("refs/remotes/origin/") {
                return name.to_string();
            }
        }
    }
    if local.iter().any(|b| b == "master") {
        "master".to_string()
    } else {
        "main".to_string()
    }
}

/// `origin/*` remote-tracking branches, excluding `origin/HEAD` and any whose
/// short name already exists locally; sorted default-first then alphabetically.
fn remote_branches(
    repo: &Repository,
    local: &[String],
    default_branch: &str,
) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for branch in repo
        .branches(Some(BranchType::Remote))
        .map_err(map_git_err)?
    {
        let (branch, _) = branch.map_err(map_git_err)?;
        let Some(name) = branch.name().map_err(map_git_err)? else {
            continue;
        };
        if !name.starts_with("origin/") || name == "origin/HEAD" {
            continue;
        }
        let short = name.strip_prefix("origin/").unwrap_or(name);
        if local.iter().any(|l| l == short) {
            continue;
        }
        out.push(name.to_string());
    }
    out.sort_by(|a, b| {
        let an = a.strip_prefix("origin/").unwrap_or(a);
        let bn = b.strip_prefix("origin/").unwrap_or(b);
        rank_default(an, default_branch)
            .cmp(&rank_default(bn, default_branch))
            .then_with(|| an.cmp(bn))
    });
    Ok(out)
}

/// Sort local branches: default first, then current, then alphabetically.
fn sort_local(mut branches: Vec<String>, default_branch: &str, current: &str) -> Vec<String> {
    branches.sort_by(|a, b| {
        rank_local(a, default_branch, current)
            .cmp(&rank_local(b, default_branch, current))
            .then_with(|| a.cmp(b))
    });
    branches
}

fn rank_local(b: &str, default_branch: &str, current: &str) -> u8 {
    if b == default_branch {
        0
    } else if b == current {
        1
    } else {
        2
    }
}

fn rank_default(name: &str, default_branch: &str) -> u8 {
    u8::from(name != default_branch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{commit_file, create_branch, init_repo};

    #[test]
    fn lists_local_branches_default_first() {
        let dir = init_repo("branches-local");
        commit_file(dir.path(), "a.txt", "x\n");
        create_branch(dir.path(), "feature");
        let result = get_branches(dir.path(), false).unwrap();
        assert!(result.remote_branches.is_empty());
        assert!(result.branches.contains(&"feature".to_string()));
        assert!(!result.current_branch.is_empty());
        // The default branch, when it is one of the local branches, sorts first.
        if result.branches.iter().any(|b| b == &result.default_branch) {
            assert_eq!(result.branches[0], result.default_branch);
        }
    }

    #[test]
    fn current_branch_sorts_before_other_non_default() {
        let dir = init_repo("branches-current");
        commit_file(dir.path(), "a.txt", "x\n");
        // Create two extra branches; checkout one so it becomes current.
        create_branch(dir.path(), "aaa-other");
        create_branch(dir.path(), "zzz-current");
        crate::testutil::checkout_branch(dir.path(), "zzz-current");
        let result = get_branches(dir.path(), false).unwrap();
        assert_eq!(result.current_branch, "zzz-current");
        let cur = result
            .branches
            .iter()
            .position(|b| b == "zzz-current")
            .unwrap();
        let other = result
            .branches
            .iter()
            .position(|b| b == "aaa-other")
            .unwrap();
        // Current ranks above a plain branch despite the later alphabetical name.
        assert!(cur < other);
    }

    #[test]
    fn branch_status_no_upstream_returns_zero_counts_and_clean() {
        let dir = init_repo("branch-status-clean");
        commit_file(dir.path(), "a.txt", "x\n");
        let repo = git2::Repository::open(dir.path()).unwrap();
        let current = current_branch(&repo);
        let result = branch_status(dir.path(), &current).unwrap();
        assert_eq!(result.branch, current);
        assert_eq!(result.current_branch, current);
        assert!(result.is_current_branch);
        assert_eq!(result.ahead, 0);
        assert_eq!(result.behind, 0);
        assert!(!result.has_uncommitted_changes);
    }

    #[test]
    fn branch_status_detects_modified_file_as_dirty() {
        let dir = init_repo("branch-status-dirty");
        commit_file(dir.path(), "a.txt", "one\n");
        crate::testutil::write_file(dir.path(), "a.txt", "two\n");
        let repo = git2::Repository::open(dir.path()).unwrap();
        let current = current_branch(&repo);
        let result = branch_status(dir.path(), &current).unwrap();
        assert!(result.has_uncommitted_changes);
    }

    #[test]
    fn branch_status_detects_untracked_file_as_dirty() {
        let dir = init_repo("branch-status-untracked");
        commit_file(dir.path(), "a.txt", "x\n");
        crate::testutil::write_file(dir.path(), "new.txt", "hi");
        let repo = git2::Repository::open(dir.path()).unwrap();
        let current = current_branch(&repo);
        let result = branch_status(dir.path(), &current).unwrap();
        assert!(result.has_uncommitted_changes);
    }

    #[test]
    fn branch_status_is_current_branch_false_for_other_branch() {
        let dir = init_repo("branch-status-other");
        commit_file(dir.path(), "a.txt", "x\n");
        create_branch(dir.path(), "feature");
        let result = branch_status(dir.path(), "feature").unwrap();
        assert_eq!(result.branch, "feature");
        assert!(!result.is_current_branch);
        assert_ne!(result.current_branch, "feature");
    }
}
