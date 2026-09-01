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

/// Per-response cap on `GitStatus.files` entries served over the wire
/// (monorepo#3635). A worktree with ~100k untracked files (mid-build
/// generated trees, nested worktrees) otherwise produces multi-megabyte
/// `git.status` frames — far past the transport's 1 MiB outbound warn
/// threshold. 5000 entries keeps the frame well under that bound while
/// remaining far more than any UI renders.
pub const MAX_STATUS_FILES: usize = 5000;

/// Project a wire `GitStatus` with `files` capped at [`MAX_STATUS_FILES`]
/// entries (monorepo#3635), preferring tracked changes (staged / modified /
/// deleted / renamed…) over untracked entries so real work is never pushed
/// out of the list by untracked noise. Relative order within each group is
/// preserved (libgit2's path order). On truncation, `files_truncated` is set
/// and `total_files` carries the full pre-cap count; an under-cap status is
/// cloned unchanged, keeping the pre-#3635 wire shape byte-for-byte.
/// Aggregate flags are untouched — they were computed over the full scan.
///
/// Takes the (typically cached) status by reference and clones only the
/// entries that survive the cap, so a ~100k-entry cached scan costs one
/// counting pass plus O(cap) clones per read — not a full-list clone that is
/// immediately discarded (the RPC cost contract's O(rows returned) rule).
#[must_use]
pub fn cap_status_files(status: &GitStatus) -> GitStatus {
    let total = status.files.len();
    if total <= MAX_STATUS_FILES {
        return status.clone();
    }
    let is_tracked = |f: &&FileStatus| f.status != GitFileStatus::Untracked;
    let tracked_kept = status
        .files
        .iter()
        .filter(is_tracked)
        .count()
        .min(MAX_STATUS_FILES);
    let mut files = Vec::with_capacity(MAX_STATUS_FILES);
    files.extend(
        status
            .files
            .iter()
            .filter(is_tracked)
            .take(tracked_kept)
            .cloned(),
    );
    files.extend(
        status
            .files
            .iter()
            .filter(|f| f.status == GitFileStatus::Untracked)
            .take(MAX_STATUS_FILES - tracked_kept)
            .cloned(),
    );
    GitStatus {
        branch: status.branch.clone(),
        ahead: status.ahead,
        behind: status.behind,
        diverged: status.diverged,
        files,
        has_uncommitted_changes: status.has_uncommitted_changes,
        has_untracked_files: status.has_untracked_files,
        files_truncated: true,
        total_files: Some(total),
        has_upstream: status.has_upstream,
        unpushed_count: status.unpushed_count,
    }
}

/// The empty status returned for remote workspaces and non-repositories,
/// matching the TS `getStatus` fallback (`branch:""`, everything zeroed).
#[must_use]
pub fn empty_status() -> GitStatus {
    GitStatus {
        branch: String::new(),
        ahead: 0,
        behind: 0,
        diverged: false,
        files: Vec::new(),
        has_uncommitted_changes: false,
        has_untracked_files: false,
        files_truncated: false,
        total_files: None,
        has_upstream: false,
        unpushed_count: None,
    }
}

/// Compute the working-tree status for the repository at `worktree_path`.
///
/// # Errors
///
/// Returns `Error::Internal` if the underlying libgit2 operation fails.
pub fn status(worktree_path: &Path) -> Result<GitStatus> {
    let started = Instant::now();
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let branch = current_branch(&repo);
    let (ahead, behind, has_upstream) = ahead_behind(&repo, &branch);
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
        scan_ms = u64::try_from(scan_elapsed.as_millis()).unwrap_or(u64::MAX),
        total_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
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
        files_truncated: false,
        total_files: None,
        has_upstream,
        // `upstream..HEAD` is exactly the `ahead` count when the upstream
        // exists; without one there is nothing to count against
        // (monorepo#4058).
        unpushed_count: has_upstream.then_some(ahead),
    })
}

/// Mirror `git branch --show-current`: the branch shorthand, empty on a detached
/// HEAD, and the unborn branch name when there is no commit yet.
#[must_use]
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
#[must_use]
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
/// is no upstream (the `git rev-list … || 0\t0` fallback). The third element
/// reports whether the upstream ref exists (monorepo#4058), so callers can
/// distinguish "even with upstream" from "no upstream at all" — both `(0, 0)`.
fn ahead_behind(repo: &Repository, branch: &str) -> (i64, i64, bool) {
    if branch.is_empty() {
        return (0, 0, false);
    }
    let Some(local) = repo.head().ok().and_then(|h| h.target()) else {
        return (0, 0, false);
    };
    let upstream_ref = format!("refs/remotes/origin/{branch}");
    let Some(upstream) = repo
        .find_reference(&upstream_ref)
        .ok()
        .and_then(|r| r.target())
    else {
        return (0, 0, false);
    };
    match repo.graph_ahead_behind(local, upstream) {
        Ok((a, b)) => (
            i64::try_from(a).expect("value fits in i64"),
            i64::try_from(b).expect("value fits in i64"),
            true,
        ),
        // Deliberate fallback: the upstream ref exists but the walk failed, so
        // report the pre-existing (0, 0) counts with `has_upstream: true` —
        // `unpushedCount` then reads `Some(0)`, preserving the documented
        // `unpushedCount == ahead` invariant over a degraded count.
        Err(_) => (0, 0, true),
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

    /// Point `refs/remotes/origin/<branch>` at the current HEAD commit — a
    /// local stand-in for a pushed branch (no network).
    fn set_origin_ref(worktree: &std::path::Path, branch: &str) {
        let repo = git2::Repository::open(worktree).unwrap();
        let head = repo.head().unwrap().target().unwrap();
        repo.reference(&format!("refs/remotes/origin/{branch}"), head, true, "test")
            .unwrap();
    }

    /// With an upstream at HEAD the branch is even: `hasUpstream: true` and
    /// `unpushedCount: 0` (monorepo#4058) — distinguishable from the
    /// no-upstream case below despite identical ahead/behind.
    #[test]
    fn upstream_even_reports_zero_unpushed() {
        let dir = init_repo("status-upstream-even");
        commit_file(dir.path(), "a.txt", "x\n");
        let branch = status(dir.path()).unwrap().branch;
        set_origin_ref(dir.path(), &branch);
        let st = status(dir.path()).unwrap();
        assert!(st.has_upstream);
        assert_eq!(st.ahead, 0);
        assert_eq!(st.unpushed_count, Some(0));
    }

    /// Commits past the upstream surface as both `ahead` and
    /// `unpushedCount` (the `upstream..HEAD` count).
    #[test]
    fn upstream_ahead_reports_unpushed_count() {
        let dir = init_repo("status-upstream-ahead");
        commit_file(dir.path(), "a.txt", "x\n");
        let branch = status(dir.path()).unwrap().branch;
        set_origin_ref(dir.path(), &branch);
        commit_file(dir.path(), "b.txt", "y\n");
        let st = status(dir.path()).unwrap();
        assert!(st.has_upstream);
        assert_eq!(st.ahead, 1);
        assert_eq!(st.unpushed_count, Some(1));
    }

    /// A never-pushed branch (no `refs/remotes/origin/<branch>`) reports
    /// `hasUpstream: false` with `unpushedCount` omitted — no longer
    /// indistinguishable from an even branch (monorepo#4058).
    #[test]
    fn missing_upstream_reports_no_upstream() {
        let dir = init_repo("status-upstream-missing");
        commit_file(dir.path(), "a.txt", "x\n");
        let st = status(dir.path()).unwrap();
        assert!(!st.has_upstream);
        assert_eq!(st.ahead, 0);
        assert_eq!(st.unpushed_count, None);
    }

    /// Detached HEAD: no branch, so no upstream to compare against.
    #[test]
    fn detached_head_reports_no_upstream() {
        let dir = init_repo("status-upstream-detached");
        commit_file(dir.path(), "a.txt", "x\n");
        let repo = git2::Repository::open(dir.path()).unwrap();
        let head = repo.head().unwrap().target().unwrap();
        repo.set_head_detached(head).unwrap();
        let st = status(dir.path()).unwrap();
        assert!(st.branch.is_empty());
        assert!(!st.has_upstream);
        assert_eq!(st.unpushed_count, None);
    }

    /// Unborn HEAD (fresh `git init`, no commit): the unborn branch name is
    /// reported but there is no local commit, hence no upstream comparison.
    #[test]
    fn unborn_head_reports_no_upstream() {
        let dir = init_repo("status-upstream-unborn");
        let st = status(dir.path()).unwrap();
        assert!(!st.has_upstream);
        assert_eq!(st.unpushed_count, None);
    }

    /// Wire additivity (monorepo#4058): `hasUpstream` is a plain
    /// always-present boolean, while `unpushedCount` is omitted without an
    /// upstream and present (even at 0) with one.
    #[test]
    fn upstream_fields_wire_shape() {
        let dir = init_repo("status-upstream-wire");
        commit_file(dir.path(), "a.txt", "x\n");
        let v = serde_json::to_value(status(dir.path()).unwrap()).unwrap();
        assert_eq!(v["hasUpstream"], serde_json::json!(false));
        assert!(v.get("unpushedCount").is_none());

        let branch = v["branch"].as_str().unwrap();
        set_origin_ref(dir.path(), branch);
        let v = serde_json::to_value(status(dir.path()).unwrap()).unwrap();
        assert_eq!(v["hasUpstream"], serde_json::json!(true));
        assert_eq!(v["unpushedCount"], serde_json::json!(0));
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

    fn file(path: &str, status: GitFileStatus, staged: bool) -> FileStatus {
        FileStatus {
            path: path.to_string(),
            status,
            staged,
            mode: None,
            old_sha: None,
            new_sha: None,
        }
    }

    fn status_with_files(files: Vec<FileStatus>) -> GitStatus {
        GitStatus {
            files,
            has_uncommitted_changes: true,
            has_untracked_files: true,
            ..empty_status()
        }
    }

    /// Under the cap the status passes through untouched: no truncation
    /// markers, files identical (the pre-#3635 wire shape).
    #[test]
    fn cap_leaves_under_cap_status_untouched() {
        let files: Vec<_> = (0..10)
            .map(|i| file(&format!("f{i}.txt"), GitFileStatus::Untracked, false))
            .collect();
        let capped = cap_status_files(&status_with_files(files.clone()));
        assert_eq!(capped.files, files);
        assert!(!capped.files_truncated);
        assert_eq!(capped.total_files, None);
    }

    /// An exactly-at-cap list is not truncated (the cap is inclusive).
    #[test]
    fn cap_is_inclusive_at_the_boundary() {
        let files: Vec<_> = (0..MAX_STATUS_FILES)
            .map(|i| file(&format!("f{i}"), GitFileStatus::Untracked, false))
            .collect();
        let capped = cap_status_files(&status_with_files(files));
        assert_eq!(capped.files.len(), MAX_STATUS_FILES);
        assert!(!capped.files_truncated);
        assert_eq!(capped.total_files, None);
    }

    /// Over the cap, tracked changes are all retained ahead of untracked
    /// entries, the list is capped, and the truncation markers carry the full
    /// pre-cap count. Aggregate flags stay as computed over the full scan.
    #[test]
    fn cap_prefers_tracked_changes_over_untracked() {
        let mut files = Vec::new();
        for i in 0..MAX_STATUS_FILES {
            files.push(file(
                &format!("untracked{i}"),
                GitFileStatus::Untracked,
                false,
            ));
        }
        // Tracked entries interleaved after the untracked block: they must
        // survive the cap even though they come last in scan order.
        files.push(file("staged.txt", GitFileStatus::Modified, true));
        files.push(file("deleted.txt", GitFileStatus::Deleted, false));
        let total = files.len();

        let capped = cap_status_files(&status_with_files(files));
        assert_eq!(capped.files.len(), MAX_STATUS_FILES);
        assert!(capped.files_truncated);
        assert_eq!(capped.total_files, Some(total));
        assert!(capped.has_uncommitted_changes);
        assert!(capped.has_untracked_files);
        assert!(capped.files.iter().any(|f| f.path == "staged.txt"));
        assert!(capped.files.iter().any(|f| f.path == "deleted.txt"));
        // Tracked entries lead the list; untracked fill the remainder in
        // their original relative order.
        assert_eq!(capped.files[0].path, "staged.txt");
        assert_eq!(capped.files[1].path, "deleted.txt");
        assert_eq!(capped.files[2].path, "untracked0");
        let untracked_kept = capped
            .files
            .iter()
            .filter(|f| f.status == GitFileStatus::Untracked)
            .count();
        assert_eq!(untracked_kept, MAX_STATUS_FILES - 2);
    }

    /// A pathological all-tracked overflow still enforces the cap.
    #[test]
    fn cap_applies_even_when_tracked_alone_overflows() {
        let files: Vec<_> = (0..MAX_STATUS_FILES + 7)
            .map(|i| file(&format!("m{i}"), GitFileStatus::Modified, false))
            .collect();
        let capped = cap_status_files(&status_with_files(files));
        assert_eq!(capped.files.len(), MAX_STATUS_FILES);
        assert!(capped.files_truncated);
        assert_eq!(capped.total_files, Some(MAX_STATUS_FILES + 7));
    }

    /// The truncation markers serialize additively: absent on an untruncated
    /// status (pre-#3635 shape byte-for-byte), present when truncated.
    #[test]
    fn truncation_markers_are_additive_on_the_wire() {
        let untruncated = serde_json::to_value(status_with_files(vec![])).unwrap();
        assert!(untruncated.get("filesTruncated").is_none());
        assert!(untruncated.get("totalFiles").is_none());

        let files: Vec<_> = (0..=MAX_STATUS_FILES)
            .map(|i| file(&format!("f{i}"), GitFileStatus::Untracked, false))
            .collect();
        let truncated = serde_json::to_value(cap_status_files(&status_with_files(files))).unwrap();
        assert_eq!(truncated["filesTruncated"], serde_json::json!(true));
        assert_eq!(
            truncated["totalFiles"],
            serde_json::json!(MAX_STATUS_FILES + 1)
        );
    }
}
