//! Branch listing (`git.getBranches`).
//!
//! Ports the TS `git.getBranches` handler: local branches, optional
//! `origin/*` remote branches, current branch, and the default branch
//! (`origin/HEAD`, falling back to `master`/`main`). The "known repo"
//! authorization check is wire policy and lives in `intent-services`.

use std::path::Path;

use git2::{BranchType, Repository};
use intent_core::{GitBranches, Result};

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
        if let Some(target) = reference.symbolic_target() {
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
}
