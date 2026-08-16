//! Working-tree status (`git.status`).
//!
//! Ports `gitService.getStatus`: branch, ahead/behind vs `origin/<branch>`,
//! `diverged`, and the porcelain file list (with the staged+unstaged
//! double-entry rule from `parseStatusOutput`).

use std::path::Path;
use std::time::Instant;

use git2::{DiffDelta, FileMode, Repository, Status, StatusOptions};
use intent_core::{FileStatus, GitFileStatus, GitStatus, Result};

use crate::map_git_err;

/// The empty status returned for remote workspaces and non-repositories,
/// matching the TS `getStatus` fallback (`branch:""`, everything zeroed).
pub fn empty_status() -> GitStatus {
    GitStatus {
        branch: String::new(),
        ahead: 0,
        behind: 0,
        diverged: false,
        files: Vec::new(),
        has_uncommitted_changes: false,
        has_untracked_files: false,
    }
}

/// Compute the working-tree status for the repository at `worktree_path`.
pub fn status(worktree_path: &Path) -> Result<GitStatus> {
    let started = Instant::now();
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let branch = current_branch(&repo);
    let (ahead, behind) = ahead_behind(&repo, &branch);
    // The canonical definition (per the `GitStatus.diverged` doc): both ahead
    // and behind the upstream.
    let diverged = ahead > 0 && behind > 0;
    let scan_started = Instant::now();
    let files = collect_files(&repo)?;
    let scan_elapsed = scan_started.elapsed();
    let has_uncommitted_changes = files
        .iter()
        .any(|f| f.staged || f.status != GitFileStatus::Untracked);
    let has_untracked_files = files.iter().any(|f| f.status == GitFileStatus::Untracked);
    tracing::debug!(
        files = files.len(),
        scan_ms = scan_elapsed.as_millis() as u64,
        total_ms = started.elapsed().as_millis() as u64,
        "status: working-tree status scan"
    );
    Ok(GitStatus {
        branch,
        ahead,
        behind,
        diverged,
        files,
        has_uncommitted_changes,
        has_untracked_files,
    })
}

/// Mirror `git branch --show-current`: the branch shorthand, empty on a detached
/// HEAD, and the unborn branch name when there is no commit yet.
pub fn current_branch(repo: &Repository) -> String {
    match repo.head() {
        Ok(head) if head.is_branch() => head.shorthand().unwrap_or("").to_string(),
        Ok(_) => String::new(),
        Err(_) => repo
            .find_reference("HEAD")
            .ok()
            .and_then(|r| r.symbolic_target().ok().flatten().map(str::to_string))
            .and_then(|t| t.strip_prefix("refs/heads/").map(str::to_string))
            .unwrap_or_default(),
    }
}

/// Path-based convenience over [`current_branch`]: open the repository at
/// `worktree_path` and return its checked-out branch. `None` when the repo
/// cannot be opened or `HEAD` is not a branch (detached / unborn).
pub fn current_branch_at(worktree_path: &Path) -> Option<String> {
    let repo = Repository::open(worktree_path).ok()?;
    let name = current_branch(&repo);
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

/// Ahead/behind counts vs `origin/<branch>`, defaulting to `(0, 0)` when there
/// is no upstream (the `git rev-list … || 0\t0` fallback).
fn ahead_behind(repo: &Repository, branch: &str) -> (i64, i64) {
    if branch.is_empty() {
        return (0, 0);
    }
    let Some(local) = repo.head().ok().and_then(|h| h.target()) else {
        return (0, 0);
    };
    let upstream_ref = format!("refs/remotes/origin/{branch}");
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

/// Build the file list from `repo.statuses`, replicating the porcelain
/// `--untracked-files=all` parse (directories skipped; staged+unstaged split).
fn collect_files(repo: &Repository) -> Result<Vec<FileStatus>> {
    let mut opts = StatusOptions::new();
    opts.include_untracked(true)
        .recurse_untracked_dirs(true)
        .include_ignored(false)
        .include_unmodified(false);
    let statuses = repo.statuses(Some(&mut opts)).map_err(map_git_err)?;
    let mut files = Vec::new();
    for entry in statuses.iter() {
        let Ok(path) = entry.path() else { continue };
        if path.ends_with('/') {
            continue;
        }
        let (index_char, wt_char) = porcelain_chars(entry.status());
        let staged_link = entry.head_to_index().as_ref().and_then(gitlink_info);
        let unstaged_link = entry.index_to_workdir().as_ref().and_then(gitlink_info);
        push_entries(
            &mut files,
            path,
            index_char,
            wt_char,
            staged_link,
            unstaged_link,
        );
    }
    Ok(files)
}

/// Gitlink metadata for one status delta: `Some((mode, old_sha, new_sha))`
/// when either side is a `160000` submodule pin, else `None` (monorepo#1739).
/// Each side's SHA is emitted only when **that side** is in gitlink mode — a
/// type change (gitlink→regular file or the reverse) keeps only the pin side,
/// never leaking the regular side's blob OID as a pin SHA. A zero OID (missing
/// side — added/deleted submodule, or a workdir side libgit2 could not
/// resolve) also folds to `None` on that side.
fn gitlink_info(delta: &DiffDelta) -> Option<(String, Option<String>, Option<String>)> {
    let commit_mode = FileMode::Commit;
    let old = delta.old_file();
    let new = delta.new_file();
    if old.mode() != commit_mode && new.mode() != commit_mode {
        return None;
    }
    let sha = |f: git2::DiffFile| {
        if f.mode() != commit_mode || f.id().is_zero() {
            None
        } else {
            Some(f.id().to_string())
        }
    };
    Some(("160000".to_string(), sha(old), sha(new)))
}

/// Reduce a git2 [`Status`] bitset to the porcelain `(X, Y)` status characters.
fn porcelain_chars(s: Status) -> (char, char) {
    let index_bits = Status::INDEX_NEW
        | Status::INDEX_MODIFIED
        | Status::INDEX_DELETED
        | Status::INDEX_RENAMED
        | Status::INDEX_TYPECHANGE;
    if s.contains(Status::WT_NEW) && !s.intersects(index_bits) {
        return ('?', '?');
    }
    if s.contains(Status::CONFLICTED) {
        return ('U', 'U');
    }
    let index = if s.contains(Status::INDEX_NEW) {
        'A'
    } else if s.contains(Status::INDEX_MODIFIED) {
        'M'
    } else if s.contains(Status::INDEX_DELETED) {
        'D'
    } else if s.contains(Status::INDEX_RENAMED) {
        'R'
    } else if s.contains(Status::INDEX_TYPECHANGE) {
        'T'
    } else {
        ' '
    };
    let wt = if s.contains(Status::WT_MODIFIED) {
        'M'
    } else if s.contains(Status::WT_DELETED) {
        'D'
    } else if s.contains(Status::WT_RENAMED) {
        'R'
    } else if s.contains(Status::WT_TYPECHANGE) {
        'T'
    } else {
        ' '
    };
    (index, wt)
}

/// Append the `FileStatus` entries for one porcelain line, replicating
/// `parseStatusOutput`: a file with both staged and unstaged changes yields two
/// entries (staged + unstaged); otherwise a single entry. `staged_link` /
/// `unstaged_link` carry the per-delta gitlink metadata from [`gitlink_info`],
/// attached to the corresponding entry when present.
fn push_entries(
    files: &mut Vec<FileStatus>,
    path: &str,
    index_status: char,
    work_tree_status: char,
    staged_link: Option<(String, Option<String>, Option<String>)>,
    unstaged_link: Option<(String, Option<String>, Option<String>)>,
) {
    let actual = if work_tree_status != ' ' {
        work_tree_status
    } else if index_status != ' ' {
        index_status
    } else {
        '?'
    };
    let has_staged = index_status != ' ' && index_status != '?';
    let has_unstaged = work_tree_status != ' ';
    let entry = |status: GitFileStatus,
                 staged: bool,
                 link: Option<(String, Option<String>, Option<String>)>| {
        let (mode, old_sha, new_sha) = match link {
            Some((mode, old, new)) => (Some(mode), old, new),
            None => (None, None, None),
        };
        FileStatus {
            path: path.to_string(),
            status,
            staged,
            mode,
            old_sha,
            new_sha,
        }
    };
    if has_staged && has_unstaged {
        if let Some(status) = char_to_status(index_status) {
            files.push(entry(status, true, staged_link));
        }
        if let Some(status) = char_to_status(work_tree_status) {
            files.push(entry(status, false, unstaged_link));
        }
    } else if let Some(status) = char_to_status(actual) {
        let link = if has_staged {
            staged_link
        } else {
            unstaged_link
        };
        files.push(entry(status, has_staged, link));
    }
}

/// Map a porcelain status character to a [`GitFileStatus`]. Typechange (`T`) and
/// unmerged (`U`) fold to `Modified` (the closest canonical TS variant), since
/// the TS `GitFileStatus` enum has no dedicated members for them.
fn char_to_status(c: char) -> Option<GitFileStatus> {
    match c {
        'M' | 'T' | 'U' => Some(GitFileStatus::Modified),
        'A' => Some(GitFileStatus::Added),
        'D' => Some(GitFileStatus::Deleted),
        'R' => Some(GitFileStatus::Renamed),
        'C' => Some(GitFileStatus::Copied),
        '?' => Some(GitFileStatus::Untracked),
        '!' => Some(GitFileStatus::Ignored),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{commit_file, init_repo, write_file};

    #[test]
    fn untracked_file_is_question_mark_unstaged() {
        let dir = init_repo("status-untracked");
        write_file(dir.path(), "new.txt", "hi");
        let st = status(dir.path()).unwrap();
        assert!(st.has_untracked_files);
        assert!(!st.has_uncommitted_changes);
        let f = st.files.iter().find(|f| f.path == "new.txt").unwrap();
        assert_eq!(f.status, GitFileStatus::Untracked);
        assert!(!f.staged);
    }

    #[test]
    fn staged_and_unstaged_modification_yields_two_entries() {
        let dir = init_repo("status-mm");
        commit_file(dir.path(), "a.txt", "one\n");
        write_file(dir.path(), "a.txt", "two\n");
        crate::stage::stage(dir.path(), &["a.txt".to_string()]).unwrap();
        write_file(dir.path(), "a.txt", "three\n");
        let st = status(dir.path()).unwrap();
        let entries: Vec<_> = st.files.iter().filter(|f| f.path == "a.txt").collect();
        assert_eq!(entries.len(), 2, "expected staged + unstaged entries");
        assert!(entries
            .iter()
            .any(|f| f.staged && f.status == GitFileStatus::Modified));
        assert!(entries
            .iter()
            .any(|f| !f.staged && f.status == GitFileStatus::Modified));
        assert!(st.has_uncommitted_changes);
    }

    #[test]
    fn branch_name_reported() {
        let dir = init_repo("status-branch");
        commit_file(dir.path(), "a.txt", "x\n");
        let st = status(dir.path()).unwrap();
        assert!(!st.branch.is_empty());
        assert_eq!(st.ahead, 0);
        assert_eq!(st.behind, 0);
        assert!(!st.diverged);
    }

    /// A submodule pin change carries `mode: "160000"` plus the old/new pin
    /// SHAs; regular file entries carry none of the gitlink metadata
    /// (monorepo#1739).
    #[test]
    fn gitlink_pin_change_carries_mode_and_shas() {
        let child = init_repo("status-gitlink-child");
        commit_file(child.path(), "c.txt", "one\n");
        let parent = init_repo("status-gitlink-parent");
        commit_file(parent.path(), "p.txt", "root\n");
        crate::testutil::add_submodule(parent.path(), child.path(), "sub");

        let sub_path = parent.path().join("sub");
        let head = |p: &std::path::Path| {
            git2::Repository::open(p)
                .unwrap()
                .head()
                .unwrap()
                .target()
                .unwrap()
                .to_string()
        };
        let old_sha = head(&sub_path);
        // Advance the submodule checkout: the workdir pin now differs from
        // the committed gitlink.
        commit_file(&sub_path, "c.txt", "two\n");
        let new_sha = head(&sub_path);
        write_file(parent.path(), "p.txt", "changed\n");

        let st = status(parent.path()).unwrap();
        let sub = st.files.iter().find(|f| f.path == "sub").unwrap();
        assert_eq!(sub.status, GitFileStatus::Modified);
        assert!(!sub.staged);
        assert_eq!(sub.mode.as_deref(), Some("160000"));
        assert_eq!(sub.old_sha.as_deref(), Some(old_sha.as_str()));
        assert_eq!(sub.new_sha.as_deref(), Some(new_sha.as_str()));

        let plain = st.files.iter().find(|f| f.path == "p.txt").unwrap();
        assert_eq!(plain.mode, None);
        assert_eq!(plain.old_sha, None);
        assert_eq!(plain.new_sha, None);
    }

    /// A gitlink→regular-file replacement (type change) keeps the pin SHA on
    /// the gitlink side only: the entry is still flagged `mode: "160000"` but
    /// the regular side's blob OID is never exposed as a pin SHA
    /// (monorepo#1739 review).
    #[test]
    fn gitlink_typechange_never_exposes_blob_as_pin() {
        let parent = init_repo("status-gitlink-typechange");
        commit_file(parent.path(), "p.txt", "root\n");
        let old_sha = "7257a190564088376227525989c4994e46082ad1";
        crate::testutil::commit_gitlink_bump(parent.path(), "sub", old_sha);
        // Stage a regular file where the gitlink was.
        let repo = git2::Repository::open(parent.path()).unwrap();
        let mut index = repo.index().unwrap();
        index.remove_path(std::path::Path::new("sub")).unwrap();
        write_file(parent.path(), "sub", "now a file\n");
        index.add_path(std::path::Path::new("sub")).unwrap();
        index.write().unwrap();

        let st = status(parent.path()).unwrap();
        let sub = st
            .files
            .iter()
            .find(|f| f.path == "sub" && f.staged)
            .unwrap();
        assert_eq!(sub.mode.as_deref(), Some("160000"));
        assert_eq!(sub.old_sha.as_deref(), Some(old_sha));
        assert_eq!(sub.new_sha, None, "blob OID must not leak as a pin SHA");
    }
}
