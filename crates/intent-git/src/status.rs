//! Working-tree status (`git.status`).
//!
//! Ports `gitService.getStatus`: branch, ahead/behind vs `origin/<branch>`,
//! `diverged`, and the porcelain file list (with the staged+unstaged
//! double-entry rule from `parseStatusOutput`).

use std::path::Path;

use git2::{Repository, Status, StatusOptions};
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
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let branch = current_branch(&repo);
    let (ahead, behind) = ahead_behind(&repo, &branch);
    // The canonical definition (per the `GitStatus.diverged` doc): both ahead
    // and behind the upstream.
    let diverged = ahead > 0 && behind > 0;
    let files = collect_files(&repo)?;
    let has_uncommitted_changes = files
        .iter()
        .any(|f| f.staged || f.status != GitFileStatus::Untracked);
    let has_untracked_files = files.iter().any(|f| f.status == GitFileStatus::Untracked);
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
pub(crate) fn current_branch(repo: &Repository) -> String {
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
        push_entries(&mut files, path, index_char, wt_char);
    }
    Ok(files)
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
/// entries (staged + unstaged); otherwise a single entry.
fn push_entries(
    files: &mut Vec<FileStatus>,
    path: &str,
    index_status: char,
    work_tree_status: char,
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
    if has_staged && has_unstaged {
        if let Some(status) = char_to_status(index_status) {
            files.push(FileStatus {
                path: path.to_string(),
                status,
                staged: true,
            });
        }
        if let Some(status) = char_to_status(work_tree_status) {
            files.push(FileStatus {
                path: path.to_string(),
                status,
                staged: false,
            });
        }
    } else if let Some(status) = char_to_status(actual) {
        files.push(FileStatus {
            path: path.to_string(),
            status,
            staged: has_staged,
        });
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
}
