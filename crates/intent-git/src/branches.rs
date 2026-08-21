//! Branch listing (`git.getBranches`) and branch-name uniquification.
//!
//! Ports the TS `git.getBranches` handler: local branches, optional
//! `origin/*` remote branches, current branch, and the default branch
//! (`origin/HEAD`, falling back to `master`/`main`). The "known repo"
//! authorization check is wire policy and lives in `intent-services`.
//! [`ensure_unique_branch_name`] ports the collision suffixing used by the
//! reference's workspace-branch generation (`workspace-slug.ts` +
//! `workspace.service.ts`): `-2`, `-3`, … until the name is free.

use std::collections::HashSet;
use std::path::Path;

use git2::{BranchType, Repository, Status, StatusOptions};
use intent_core::{Error, GitBranchStatus, GitBranches, Result};

use crate::map_git_err;
use crate::status::current_branch;

/// List branches for the repository at `repo_path`.
///
/// # Errors
///
/// Returns `Error::Internal` if the underlying libgit2 operation fails.
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
///
/// # Errors
///
/// Returns `Error::Internal` if the underlying libgit2 operation fails.
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

/// Return `desired` if no local or remote-tracking branch already uses it,
/// else the first free `desired-N` (N starting at 2) — TS parity with the
/// reference's collision handling when auto-naming workspace branches.
/// Remote-tracking names are compared by their short name (`origin/foo` ⇒
/// `foo`) so a branch that only exists on the remote still forces a suffix.
///
/// # Errors
///
/// Returns `Error::Internal` if the repository cannot be opened or the branch list cannot be read.
pub fn ensure_unique_branch_name(repo_path: &Path, desired: &str) -> Result<String> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let taken = existing_branch_names(&repo)?;
    if !taken.contains(desired) {
        return Ok(desired.to_string());
    }
    let mut n: u32 = 2;
    loop {
        let candidate = format!("{desired}-{n}");
        if !taken.contains(candidate.as_str()) {
            return Ok(candidate);
        }
        n += 1;
    }
}

/// Force-delete a local branch (`git branch -D` parity). Used by the
/// `workspace.delete` cleanup after its worktree is removed; the caller owns
/// the guard deciding *whether* the branch may be deleted.
///
/// # Errors
///
/// Returns `Error::Internal` if the branch does not exist or the deletion fails.
pub fn delete_local_branch(repo_path: &Path, branch: &str) -> Result<()> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let mut b = repo
        .find_branch(branch, BranchType::Local)
        .map_err(map_git_err)?;
    b.delete().map_err(map_git_err)
}

/// Create a new local branch pointing at `HEAD` and optionally check it out
/// (`git branch <name>` / `git checkout -b <name>` parity). Errors when the
/// branch already exists or `HEAD` is unborn. Ports
/// `gitService.createBranch`.
///
/// # Errors
///
/// Returns `Error::InvalidParams` if `branch_name` is empty; `Error::Internal` if the branch already exists, `HEAD` is unborn, or another libgit2 operation fails.
pub fn create_branch(repo_path: &Path, branch_name: &str, checkout: bool) -> Result<()> {
    if branch_name.is_empty() {
        return Err(Error::InvalidParams(
            "branch name cannot be empty".to_string(),
        ));
    }
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let head = repo
        .head()
        .map_err(map_git_err)?
        .peel_to_commit()
        .map_err(map_git_err)?;
    repo.branch(branch_name, &head, false)
        .map_err(map_git_err)?;
    if checkout {
        let refname = format!("refs/heads/{branch_name}");
        let obj = repo.revparse_single(&refname).map_err(map_git_err)?;
        repo.checkout_tree(&obj, None).map_err(map_git_err)?;
        repo.set_head(&refname).map_err(map_git_err)?;
    }
    Ok(())
}

/// Create `branch_name` at `base_ref` (local branch, then any rev-parsable
/// spec — tag/SHA) and check it out. Errors with [`Error::BaseRefUnresolvable`]
/// when the base ref does not resolve, and when the branch already exists.
/// Used by the `isNewRepo` create arm, which works directly in the
/// repository folder (no worktree) but must still honor a caller-supplied
/// `baseRef` (`provision_worktree` parity).
///
/// # Errors
///
/// Returns `Error::InvalidParams` if `branch_name` is empty; [`Error::BaseRefUnresolvable`] if `base_ref` does not resolve; `Error::Internal` if the branch already exists or the checkout fails.
pub fn create_branch_at(repo_path: &Path, branch_name: &str, base_ref: &str) -> Result<()> {
    if branch_name.is_empty() {
        return Err(Error::InvalidParams(
            "branch name cannot be empty".to_string(),
        ));
    }
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let base_commit = [format!("refs/heads/{base_ref}"), base_ref.to_string()]
        .iter()
        .find_map(|spec| repo.revparse_single(spec).ok())
        .and_then(|obj| obj.peel_to_commit().ok())
        .ok_or_else(|| Error::BaseRefUnresolvable {
            base_ref: base_ref.to_string(),
        })?;
    repo.branch(branch_name, &base_commit, false)
        .map_err(map_git_err)?;
    let refname = format!("refs/heads/{branch_name}");
    let obj = repo.revparse_single(&refname).map_err(map_git_err)?;
    repo.checkout_tree(&obj, None).map_err(map_git_err)?;
    repo.set_head(&refname).map_err(map_git_err)?;
    Ok(())
}

/// Check out an existing local branch (`git checkout <name>` parity). Errors
/// when the branch is missing. Ports `gitService.checkoutBranch`.
///
/// # Errors
///
/// Returns `Error::InvalidParams` if `branch_name` is empty; `Error::Internal` if the branch is missing or the checkout fails.
pub fn checkout_branch(repo_path: &Path, branch_name: &str) -> Result<()> {
    if branch_name.is_empty() {
        return Err(Error::InvalidParams(
            "branch name cannot be empty".to_string(),
        ));
    }
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let refname = format!("refs/heads/{branch_name}");
    let obj = repo.revparse_single(&refname).map_err(map_git_err)?;
    repo.checkout_tree(&obj, None).map_err(map_git_err)?;
    repo.set_head(&refname).map_err(map_git_err)?;
    Ok(())
}

/// Rename `old_name` → `new_name` (`git branch -m` parity). Errors when the
/// old branch is missing or the new name is already in use. Ports
/// `gitService.renameBranch`; the FE's format validation (`git
/// check-ref-format`) and `-32602` wire-policy live in `intent-services`.
///
/// # Errors
///
/// Returns `Error::InvalidParams` if `new_name` is empty; `Error::Internal` if the old branch is missing or the new name is already in use.
pub fn rename_branch(repo_path: &Path, old_name: &str, new_name: &str) -> Result<()> {
    if new_name.is_empty() {
        return Err(Error::InvalidParams(
            "new branch name cannot be empty".to_string(),
        ));
    }
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    let mut b = repo
        .find_branch(old_name, BranchType::Local)
        .map_err(map_git_err)?;
    b.rename(new_name, false).map_err(map_git_err)?;
    Ok(())
}

/// All branch names occupied in the repo: local names plus the short names of
/// remote-tracking branches (any remote, `remote/` prefix stripped).
fn existing_branch_names(repo: &Repository) -> Result<HashSet<String>> {
    let mut names = HashSet::new();
    for branch in repo.branches(None).map_err(map_git_err)? {
        let (branch, kind) = branch.map_err(map_git_err)?;
        let Some(name) = branch.name().map_err(map_git_err)? else {
            continue;
        };
        match kind {
            BranchType::Local => {
                names.insert(name.to_string());
            }
            BranchType::Remote => {
                if name.ends_with("/HEAD") {
                    continue;
                }
                let short = name.split_once('/').map_or(name, |(_, s)| s);
                names.insert(short.to_string());
            }
        }
    }
    Ok(names)
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

/// The repository's actual default branch when determinable: `origin/HEAD`'s
/// symbolic target, else the branch HEAD points at. Unlike the listing
/// default above (which guesses `master`/`main`), this errors when neither
/// source is available so callers can apply their own last-resort fallback.
/// HEAD is read via its symbolic target (not `repo.head()`) so an unborn
/// branch — a freshly `git init`ed repo with no commits — still yields its
/// real initial branch name; only a detached HEAD (not symbolic) errors.
/// Backs the propose-time empty-branch default for chief workspace-create
/// proposals (monorepo#761).
///
/// # Errors
///
/// Returns `Error::Internal` if the repository cannot be opened, or when neither `origin/HEAD` nor a symbolic `HEAD` yields a branch name (detached `HEAD`).
pub fn repo_default_branch(repo_path: &Path) -> Result<String> {
    let repo = Repository::open(repo_path).map_err(map_git_err)?;
    if let Ok(reference) = repo.find_reference("refs/remotes/origin/HEAD") {
        if let Ok(Some(target)) = reference.symbolic_target() {
            if let Some(name) = target.strip_prefix("refs/remotes/origin/") {
                if !name.is_empty() {
                    return Ok(name.to_string());
                }
            }
        }
    }
    if let Ok(head) = repo.find_reference("HEAD") {
        if let Ok(Some(target)) = head.symbolic_target() {
            if let Some(name) = target.strip_prefix("refs/heads/") {
                if !name.is_empty() {
                    return Ok(name.to_string());
                }
            }
        }
    }
    Err(Error::Internal(
        "cannot determine default branch (no origin/HEAD and HEAD is not on a branch)".to_string(),
    ))
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
    use crate::testutil::{commit_file, create_branch as create_branch_util, init_repo};

    #[test]
    fn repo_default_branch_prefers_origin_head_then_head() {
        let dir = init_repo("branches-repo-default");
        commit_file(dir.path(), "a.txt", "x\n");
        let repo = Repository::open(dir.path()).unwrap();
        let head_branch = repo
            .head()
            .unwrap()
            .shorthand()
            .expect("branch name")
            .to_string();

        // No origin/HEAD → the branch HEAD points at.
        assert_eq!(repo_default_branch(dir.path()).unwrap(), head_branch);

        // origin/HEAD's symbolic target wins over HEAD.
        repo.reference_symbolic(
            "refs/remotes/origin/HEAD",
            "refs/remotes/origin/trunk",
            false,
            "test",
        )
        .unwrap();
        assert_eq!(repo_default_branch(dir.path()).unwrap(), "trunk");
    }

    #[test]
    fn repo_default_branch_errors_on_detached_head() {
        let dir = init_repo("branches-detached");
        commit_file(dir.path(), "a.txt", "x\n");
        let repo = Repository::open(dir.path()).unwrap();
        let oid = repo.head().unwrap().target().unwrap();
        repo.set_head_detached(oid).unwrap();
        assert!(repo_default_branch(dir.path()).is_err());
        assert!(repo_default_branch(Path::new("/no/such/repo")).is_err());
    }

    #[test]
    fn repo_default_branch_reads_unborn_head_branch() {
        // Freshly-initialised repo, no commits: HEAD is an unborn symbolic
        // ref; its target branch name is still the repo's default.
        let dir = init_repo("branches-unborn");
        let repo = Repository::open(dir.path()).unwrap();
        repo.set_head("refs/heads/trunk").unwrap();
        assert_eq!(repo_default_branch(dir.path()).unwrap(), "trunk");
    }

    #[test]
    fn lists_local_branches_default_first() {
        let dir = init_repo("branches-local");
        commit_file(dir.path(), "a.txt", "x\n");
        create_branch_util(dir.path(), "feature");
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
        create_branch_util(dir.path(), "aaa-other");
        create_branch_util(dir.path(), "zzz-current");
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
        create_branch_util(dir.path(), "feature");
        let result = branch_status(dir.path(), "feature").unwrap();
        assert_eq!(result.branch, "feature");
        assert!(!result.is_current_branch);
        assert_ne!(result.current_branch, "feature");
    }

    /// Create a remote-tracking ref `refs/remotes/origin/<name>` at HEAD.
    fn create_remote_tracking(path: &Path, name: &str) {
        let repo = git2::Repository::open(path).unwrap();
        let head = repo.head().unwrap().target().unwrap();
        repo.reference(
            &format!("refs/remotes/origin/{name}"),
            head,
            true,
            "test remote-tracking ref",
        )
        .unwrap();
    }

    #[test]
    fn unique_branch_name_free_name_is_unchanged() {
        let dir = init_repo("unique-free");
        commit_file(dir.path(), "a.txt", "x\n");
        assert_eq!(
            ensure_unique_branch_name(dir.path(), "auth-fix").unwrap(),
            "auth-fix"
        );
    }

    #[test]
    fn unique_branch_name_suffixes_on_local_collision() {
        let dir = init_repo("unique-local");
        commit_file(dir.path(), "a.txt", "x\n");
        create_branch_util(dir.path(), "auth-fix");
        assert_eq!(
            ensure_unique_branch_name(dir.path(), "auth-fix").unwrap(),
            "auth-fix-2"
        );
        create_branch_util(dir.path(), "auth-fix-2");
        assert_eq!(
            ensure_unique_branch_name(dir.path(), "auth-fix").unwrap(),
            "auth-fix-3"
        );
    }

    #[test]
    fn unique_branch_name_suffixes_on_remote_only_collision() {
        let dir = init_repo("unique-remote");
        commit_file(dir.path(), "a.txt", "x\n");
        create_remote_tracking(dir.path(), "feature/auth-fix");
        assert_eq!(
            ensure_unique_branch_name(dir.path(), "feature/auth-fix").unwrap(),
            "feature/auth-fix-2"
        );
    }

    #[test]
    fn unique_branch_name_ignores_origin_head() {
        let dir = init_repo("unique-origin-head");
        commit_file(dir.path(), "a.txt", "x\n");
        create_remote_tracking(dir.path(), "HEAD");
        assert_eq!(
            ensure_unique_branch_name(dir.path(), "HEAD").unwrap(),
            "HEAD"
        );
    }

    #[test]
    fn create_branch_without_checkout_leaves_head_alone() {
        let dir = init_repo("create-branch-no-checkout");
        commit_file(dir.path(), "a.txt", "x\n");
        let repo = Repository::open(dir.path()).unwrap();
        let before = current_branch(&repo);
        create_branch(dir.path(), "feature", false).unwrap();
        let after = current_branch(&repo);
        assert_eq!(before, after);
        assert!(repo.find_branch("feature", BranchType::Local).is_ok());
    }

    #[test]
    fn create_branch_with_checkout_switches_head() {
        let dir = init_repo("create-branch-checkout");
        commit_file(dir.path(), "a.txt", "x\n");
        create_branch(dir.path(), "feature", true).unwrap();
        let repo = Repository::open(dir.path()).unwrap();
        assert_eq!(current_branch(&repo), "feature");
    }

    #[test]
    fn create_branch_duplicate_is_error() {
        let dir = init_repo("create-branch-dup");
        commit_file(dir.path(), "a.txt", "x\n");
        create_branch(dir.path(), "feature", false).unwrap();
        assert!(create_branch(dir.path(), "feature", false).is_err());
    }

    #[test]
    fn checkout_branch_switches_head() {
        let dir = init_repo("checkout-branch");
        commit_file(dir.path(), "a.txt", "x\n");
        create_branch(dir.path(), "feature", false).unwrap();
        checkout_branch(dir.path(), "feature").unwrap();
        let repo = Repository::open(dir.path()).unwrap();
        assert_eq!(current_branch(&repo), "feature");
    }

    #[test]
    fn checkout_branch_missing_is_error() {
        let dir = init_repo("checkout-missing");
        commit_file(dir.path(), "a.txt", "x\n");
        assert!(checkout_branch(dir.path(), "does-not-exist").is_err());
    }

    #[test]
    fn rename_branch_swaps_the_name() {
        let dir = init_repo("rename-branch");
        commit_file(dir.path(), "a.txt", "x\n");
        create_branch(dir.path(), "old", false).unwrap();
        rename_branch(dir.path(), "old", "new").unwrap();
        let repo = Repository::open(dir.path()).unwrap();
        assert!(repo.find_branch("old", BranchType::Local).is_err());
        assert!(repo.find_branch("new", BranchType::Local).is_ok());
    }

    #[test]
    fn rename_branch_to_existing_name_is_error() {
        let dir = init_repo("rename-collide");
        commit_file(dir.path(), "a.txt", "x\n");
        create_branch(dir.path(), "one", false).unwrap();
        create_branch(dir.path(), "two", false).unwrap();
        assert!(rename_branch(dir.path(), "one", "two").is_err());
    }
}
