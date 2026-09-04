//! Local-work signals for one repository (`workspace.localChanges`): the
//! branch, whether any `refs/remotes/*` ref exists, the number of commits no
//! remote ref reaches, and the number of paths with uncommitted changes.
//!
//! "Unpushed" here is remote-ref-relative, not upstream-relative: a revwalk
//! from `HEAD` hiding every `refs/remotes/*` ref. Unlike `git.status.ahead`
//! (which is `0` without an `origin/<branch>` upstream) this counts a
//! never-pushed branch's commits exactly, and a repository with no remote refs
//! at all counts its whole history — capped at [`MAX_UNPUSHED_COUNT`].

use std::collections::HashSet;
use std::path::Path;

use git2::{Oid, Repository};
use intent_core::Result;
use serde::Serialize;

use crate::map_git_err;
use crate::status::collect_files;

/// Saturation point for [`LocalChanges::unpushed_count`]: the walk stops once
/// this many commits are counted, so an unpushed history of any size costs a
/// bounded amount of work.
pub const MAX_UNPUSHED_COUNT: u64 = 1000;

/// Per-root local-work signals — the wire row of `workspace.localChanges`
/// minus the fields the service adds (`kind`, `gitRootId`, `path`, `error`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalChanges {
    /// Checked-out branch; omitted on a detached or unborn `HEAD`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// Whether the repository has at least one `refs/remotes/*` ref.
    pub has_remote_refs: bool,
    /// Commits reachable from `HEAD` but from no `refs/remotes/*` ref,
    /// saturating at [`MAX_UNPUSHED_COUNT`].
    pub unpushed_count: u64,
    /// Distinct paths with staged, unstaged, or untracked status — the same
    /// entry set `git.status.files` reports, counted once per path.
    pub uncommitted_count: u64,
}

/// Compute the local-work signals for the repository at `worktree_path`.
///
/// # Errors
///
/// Returns `Error::Internal` when `worktree_path` is not a git repository or
/// the underlying libgit2 operation fails.
pub fn local_changes(worktree_path: &Path) -> Result<LocalChanges> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let head = repo.head().ok();
    let branch = head
        .as_ref()
        .filter(|h| h.is_branch())
        .and_then(|h| h.shorthand().ok().map(str::to_string));
    let (has_remote_refs, remote_tips) = remote_tips(&repo)?;
    let unpushed_count = match head.as_ref().and_then(git2::Reference::target) {
        Some(head_oid) => count_unpushed(&repo, head_oid, &remote_tips)?,
        None => 0,
    };
    let uncommitted_count = collect_files(&repo)?
        .iter()
        .map(|f| f.path.as_str())
        .collect::<HashSet<_>>()
        .len();
    Ok(LocalChanges {
        branch,
        has_remote_refs,
        unpushed_count,
        uncommitted_count: u64::try_from(uncommitted_count).unwrap_or(u64::MAX),
    })
}

/// Every `refs/remotes/*` ref: whether any exists, plus the commit OIDs they
/// resolve to. A ref that cannot be resolved (a dangling `origin/HEAD` symref
/// after its target branch was deleted) still counts as existing but hides
/// nothing.
fn remote_tips(repo: &Repository) -> Result<(bool, Vec<Oid>)> {
    let mut any = false;
    let mut tips = Vec::new();
    for reference in repo
        .references_glob("refs/remotes/*")
        .map_err(map_git_err)?
    {
        let Ok(reference) = reference else { continue };
        any = true;
        if let Some(oid) = reference.resolve().ok().and_then(|r| r.target()) {
            tips.push(oid);
        }
    }
    Ok((any, tips))
}

/// Walk from `head` hiding every remote tip, counting commits up to
/// [`MAX_UNPUSHED_COUNT`].
fn count_unpushed(repo: &Repository, head: Oid, remote_tips: &[Oid]) -> Result<u64> {
    let mut walk = repo.revwalk().map_err(map_git_err)?;
    walk.push(head).map_err(map_git_err)?;
    for &oid in remote_tips {
        // A remote ref that does not point at a commit cannot hide anything;
        // skipping it degrades to over-counting rather than failing the row.
        let _ = walk.hide(oid);
    }
    let mut count = 0u64;
    for oid in walk {
        oid.map_err(map_git_err)?;
        count += 1;
        if count >= MAX_UNPUSHED_COUNT {
            break;
        }
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{checkout_branch, commit_file, create_branch, init_repo, write_file};

    /// Point `refs/remotes/origin/<branch>` at the current HEAD commit — a
    /// local stand-in for a pushed branch (no network).
    fn set_origin_ref(worktree: &Path, branch: &str) {
        let repo = Repository::open(worktree).unwrap();
        let head = repo.head().unwrap().target().unwrap();
        repo.reference(&format!("refs/remotes/origin/{branch}"), head, true, "test")
            .unwrap();
    }

    fn head_branch(worktree: &Path) -> String {
        crate::status::current_branch_at(worktree).unwrap()
    }

    /// Advance HEAD by `n` commits that reuse the current tree (no worktree
    /// writes), for fixtures that need many commits cheaply.
    fn add_empty_commits(worktree: &Path, n: usize) {
        let repo = Repository::open(worktree).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        for i in 0..n {
            let parent = repo.head().unwrap().peel_to_commit().unwrap();
            let tree = parent.tree().unwrap();
            repo.commit(
                Some("HEAD"),
                &sig,
                &sig,
                &format!("empty {i}"),
                &tree,
                &[&parent],
            )
            .unwrap();
        }
    }

    #[test]
    fn clean_repo_even_with_upstream_reports_zeroes() {
        let dir = init_repo("local-changes-even");
        commit_file(dir.path(), "a.txt", "x\n");
        let branch = head_branch(dir.path());
        set_origin_ref(dir.path(), &branch);
        let lc = local_changes(dir.path()).unwrap();
        assert_eq!(lc.branch.as_deref(), Some(branch.as_str()));
        assert!(lc.has_remote_refs);
        assert_eq!(lc.unpushed_count, 0);
        assert_eq!(lc.uncommitted_count, 0);
    }

    #[test]
    fn commits_ahead_of_upstream_are_unpushed() {
        let dir = init_repo("local-changes-ahead");
        commit_file(dir.path(), "a.txt", "x\n");
        set_origin_ref(dir.path(), &head_branch(dir.path()));
        commit_file(dir.path(), "b.txt", "y\n");
        commit_file(dir.path(), "c.txt", "z\n");
        let lc = local_changes(dir.path()).unwrap();
        assert!(lc.has_remote_refs);
        assert_eq!(lc.unpushed_count, 2);
        assert_eq!(lc.uncommitted_count, 0);
    }

    /// A never-pushed branch cut from a fetched remote base counts only its
    /// own commits — the base reachable from `origin/<default>` is hidden even
    /// though the branch itself has no upstream (where `git.status.ahead`
    /// reads 0).
    #[test]
    fn never_pushed_branch_counts_only_new_commits() {
        let dir = init_repo("local-changes-never-pushed");
        commit_file(dir.path(), "a.txt", "x\n");
        commit_file(dir.path(), "b.txt", "y\n");
        set_origin_ref(dir.path(), &head_branch(dir.path()));
        create_branch(dir.path(), "feat/new");
        checkout_branch(dir.path(), "feat/new");
        commit_file(dir.path(), "c.txt", "z\n");
        let lc = local_changes(dir.path()).unwrap();
        assert_eq!(lc.branch.as_deref(), Some("feat/new"));
        assert!(lc.has_remote_refs);
        assert_eq!(lc.unpushed_count, 1);
    }

    /// Without any remote ref the whole history is unpushed.
    #[test]
    fn no_remote_refs_counts_full_history() {
        let dir = init_repo("local-changes-no-remote");
        commit_file(dir.path(), "a.txt", "x\n");
        commit_file(dir.path(), "b.txt", "y\n");
        commit_file(dir.path(), "c.txt", "z\n");
        let lc = local_changes(dir.path()).unwrap();
        assert!(!lc.has_remote_refs);
        assert_eq!(lc.unpushed_count, 3);
    }

    #[test]
    fn unpushed_count_saturates_at_cap() {
        let dir = init_repo("local-changes-cap");
        commit_file(dir.path(), "a.txt", "x\n");
        add_empty_commits(dir.path(), usize::try_from(MAX_UNPUSHED_COUNT).unwrap() + 5);
        let lc = local_changes(dir.path()).unwrap();
        assert_eq!(lc.unpushed_count, MAX_UNPUSHED_COUNT);
    }

    /// Staged, unstaged, and untracked entries all count, and a path carrying
    /// both a staged and an unstaged change (two `git.status.files` entries)
    /// counts once.
    #[test]
    fn uncommitted_paths_are_counted_once_each() {
        let dir = init_repo("local-changes-uncommitted");
        commit_file(dir.path(), "a.txt", "one\n");
        commit_file(dir.path(), "b.txt", "one\n");
        // a.txt: staged + unstaged (two status entries, one path).
        write_file(dir.path(), "a.txt", "two\n");
        crate::stage::stage(dir.path(), &["a.txt".to_string()]).unwrap();
        write_file(dir.path(), "a.txt", "three\n");
        // b.txt: unstaged only.
        write_file(dir.path(), "b.txt", "two\n");
        // c.txt: staged new file.
        write_file(dir.path(), "c.txt", "new\n");
        crate::stage::stage(dir.path(), &["c.txt".to_string()]).unwrap();
        // d.txt: untracked.
        write_file(dir.path(), "nested/d.txt", "untracked\n");

        let files = crate::status::status(dir.path()).unwrap().files;
        assert_eq!(files.iter().filter(|f| f.path == "a.txt").count(), 2);

        let lc = local_changes(dir.path()).unwrap();
        assert_eq!(lc.uncommitted_count, 4);
    }

    /// Detached HEAD: no branch, but both counts are still computed.
    #[test]
    fn detached_head_omits_branch_but_counts() {
        let dir = init_repo("local-changes-detached");
        commit_file(dir.path(), "a.txt", "x\n");
        commit_file(dir.path(), "b.txt", "y\n");
        let repo = Repository::open(dir.path()).unwrap();
        let head = repo.head().unwrap().target().unwrap();
        repo.set_head_detached(head).unwrap();
        write_file(dir.path(), "c.txt", "untracked\n");
        let lc = local_changes(dir.path()).unwrap();
        assert_eq!(lc.branch, None);
        assert!(!lc.has_remote_refs);
        assert_eq!(lc.unpushed_count, 2);
        assert_eq!(lc.uncommitted_count, 1);
    }

    /// Unborn HEAD (fresh `git init`): no branch to report, nothing to walk,
    /// untracked files still counted.
    #[test]
    fn unborn_head_omits_branch_with_zero_unpushed() {
        let dir = init_repo("local-changes-unborn");
        write_file(dir.path(), "a.txt", "x\n");
        let lc = local_changes(dir.path()).unwrap();
        assert_eq!(lc.branch, None);
        assert!(!lc.has_remote_refs);
        assert_eq!(lc.unpushed_count, 0);
        assert_eq!(lc.uncommitted_count, 1);
    }

    /// A plain directory outside any repository and a missing path both fail
    /// to open.
    #[test]
    fn non_repository_path_is_an_error() {
        let outside = std::env::temp_dir().join(format!(
            "intent-git-local-changes-outside-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&outside).unwrap();
        let result = local_changes(&outside);
        let _ = std::fs::remove_dir_all(&outside);
        assert!(result.is_err());
        assert!(local_changes(&outside.join("missing")).is_err());
    }

    /// Wire shape: camelCase keys, `branch` omitted when absent.
    #[test]
    fn serializes_camel_case_and_omits_missing_branch() {
        let dir = init_repo("local-changes-wire");
        commit_file(dir.path(), "a.txt", "x\n");
        let branch = head_branch(dir.path());
        let v = serde_json::to_value(local_changes(dir.path()).unwrap()).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "branch": branch,
                "hasRemoteRefs": false,
                "unpushedCount": 1,
                "uncommittedCount": 0,
            })
        );

        let repo = Repository::open(dir.path()).unwrap();
        let head = repo.head().unwrap().target().unwrap();
        repo.set_head_detached(head).unwrap();
        let v = serde_json::to_value(local_changes(dir.path()).unwrap()).unwrap();
        assert!(v.get("branch").is_none());
        assert_eq!(v["unpushedCount"], serde_json::json!(1));
    }
}
