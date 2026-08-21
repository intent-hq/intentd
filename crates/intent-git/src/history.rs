//! Commit history (`file-tracking.loadCommits`).
//!
//! Ports the `git log --first-parent --no-merges -n <limit>` read the TS
//! file-tracking `loadCommits` handler performs: the linear first-parent history
//! from `HEAD`, with each commit carrying its changed-file list, push state
//! (against `origin/<branch>`), and the agent-attribution trailers
//! (`Agent-Id:` / `Linked-Note-Id:`) parsed from the commit body. Returned to the
//! wire as `CommitWithAttribution[]` by `intent-services` (M4.8).

use std::path::Path;
use std::time::{Duration, Instant};

use git2::{Commit, Oid, Patch, Repository, Sort};
use intent_core::{iso_from_unix_secs, Error, Result};

use crate::status::current_branch;
use crate::{map_git_err, SLOW_GIT_WARN_THRESHOLD};

/// One commit's history record, the BE shape behind `CommitWithAttribution`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRecord {
    pub hash: String,
    pub message: String,
    pub author: String,
    pub author_email: String,
    pub date: String,
    /// Changed-file paths (a tree diff vs the first parent), or `None` when the
    /// caller skipped the per-commit diff (`include_files = false`).
    pub files: Option<Vec<String>>,
    /// `files.len()` when `files` is `Some`; `0` when the diff was skipped
    /// (`files == None` is the signal that the diff didn't run — an empty
    /// commit also yields `0` here with `files == Some([])`).
    pub files_changed: usize,
    pub is_pushed: bool,
    pub agent_id: Option<String>,
    pub linked_note_id: Option<String>,
}

/// Read up to `limit` commits of first-parent, non-merge history from `HEAD`,
/// newest first. An empty repository (unborn `HEAD`) yields an empty list.
///
/// # Errors
///
/// Returns `Error::Internal` if the underlying libgit2 operation fails.
pub fn history(worktree_path: &Path, limit: usize) -> Result<Vec<CommitRecord>> {
    history_since(worktree_path, None, limit, true)
}

/// Like [`history`] but, when `base_ref` resolves, hides it from the walk so the
/// result is the `base_ref..HEAD` range (the accept-changes `localCommits`:
/// commits on the branch not yet on trunk). An unresolvable `base_ref` falls back
/// to the full `HEAD` history. With `include_files = false` the per-commit tree
/// diff is skipped entirely — `files` is `None` — which keeps the walk cheap for
/// callers that only need commit metadata (e.g. `accept-changes.getStatus`).
///
/// # Errors
///
/// Returns `Error::Internal` if the revwalk fails or another libgit2 operation fails.
pub fn history_since(
    worktree_path: &Path,
    base_ref: Option<&str>,
    limit: usize,
    include_files: bool,
) -> Result<Vec<CommitRecord>> {
    let started = Instant::now();
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    if repo.head().ok().and_then(|h| h.target()).is_none() {
        return Ok(Vec::new());
    }

    let branch = current_branch(&repo);
    let mut pushed = PushedCheck::new(upstream_tip(&repo, &branch));

    let mut walk = repo.revwalk().map_err(map_git_err)?;
    walk.push_head().map_err(map_git_err)?;
    if let Some(base) = base_ref {
        if let Ok(obj) = repo.revparse_single(base) {
            let _ = walk.hide(obj.id());
        }
    }
    walk.simplify_first_parent().map_err(map_git_err)?;
    walk.set_sorting(Sort::TIME).map_err(map_git_err)?;

    let timing_enabled = tracing::enabled!(tracing::Level::DEBUG);
    let mut diff_elapsed = Duration::ZERO;
    let mut pushed_elapsed = Duration::ZERO;
    let mut out = Vec::new();
    for oid in walk {
        if out.len() >= limit {
            break;
        }
        let oid = oid.map_err(map_git_err)?;
        let commit = repo.find_commit(oid).map_err(map_git_err)?;
        // Skip merge commits, mirroring `git log --no-merges`.
        if commit.parent_count() > 1 {
            continue;
        }
        let hash = oid.to_string();
        let pushed_started = timing_enabled.then(Instant::now);
        let is_pushed = pushed.check(&repo, oid);
        if let Some(t) = pushed_started {
            pushed_elapsed += t.elapsed();
        }
        let (agent_id, linked_note_id) = parse_trailers(commit.body().ok().flatten().unwrap_or(""));
        let files = if include_files {
            let diff_started = timing_enabled.then(Instant::now);
            let files = changed_files(&repo, &commit)?;
            if let Some(t) = diff_started {
                diff_elapsed += t.elapsed();
            }
            Some(files)
        } else {
            None
        };
        let files_changed = files.as_ref().map_or(0, Vec::len);
        let author = commit.author();
        out.push(CommitRecord {
            hash,
            message: commit.summary().ok().flatten().unwrap_or("").to_string(),
            author: author.name().unwrap_or("").to_string(),
            author_email: author.email().unwrap_or("").to_string(),
            date: iso_from_unix_secs(commit.time().seconds()),
            files,
            files_changed,
            is_pushed,
            agent_id,
            linked_note_id,
        });
    }
    let total = started.elapsed();
    tracing::debug!(
        commits = out.len(),
        limit,
        base_ref = base_ref.unwrap_or(""),
        include_files,
        pushed_check_ms = u64::try_from(pushed_elapsed.as_millis()).unwrap_or(u64::MAX),
        per_commit_diff_ms = u64::try_from(diff_elapsed.as_millis()).unwrap_or(u64::MAX),
        other_ms = u64::try_from(
            total
                .saturating_sub(diff_elapsed)
                .saturating_sub(pushed_elapsed)
                .as_millis()
        )
        .unwrap_or(u64::MAX),
        total_ms = u64::try_from(total.as_millis()).unwrap_or(u64::MAX),
        "history_since: revwalk + per-commit tree diffs"
    );
    if total >= SLOW_GIT_WARN_THRESHOLD {
        tracing::warn!(
            worktree = %worktree_path.display(),
            limit,
            commits = out.len(),
            total_ms = u64::try_from(total.as_millis()).unwrap_or(u64::MAX),
            "history_since: slow history read"
        );
    }
    Ok(out)
}

/// The tip of `refs/remotes/origin/<branch>`, or `None` when the branch is
/// unknown or has no such remote-tracking ref (the TS `hasUpstream` fallback:
/// every commit is then treated as unpushed).
fn upstream_tip(repo: &Repository, branch: &str) -> Option<Oid> {
    if branch.is_empty() {
        return None;
    }
    let upstream = format!("refs/remotes/origin/{branch}");
    repo.find_reference(&upstream).ok().and_then(|r| r.target())
}

/// Pushed-ness checker for one first-parent history walk. A commit is pushed
/// iff it is the upstream tip itself or an ancestor of it, checked per emitted
/// commit instead of pre-walking the whole `origin/<branch>..HEAD` range, so
/// the cost is bounded by the caller's page size rather than the branch
/// divergence (#963). The walk emits a linear first-parent chain
/// newest-to-oldest, so pushed status is monotone along it (every ancestor of
/// a pushed commit is also an ancestor of the upstream tip): after the first
/// pushed commit the remaining checks short-circuit, keeping the
/// behind-upstream case (every commit pushed) at one ancestry query per walk.
/// An ancestry-check error is treated as unpushed. With no upstream every
/// commit is unpushed (the TS `hasUpstream` fallback).
struct PushedCheck {
    upstream_tip: Option<Oid>,
    chain_pushed: bool,
}

impl PushedCheck {
    fn new(upstream_tip: Option<Oid>) -> Self {
        Self {
            upstream_tip,
            chain_pushed: false,
        }
    }

    /// Whether `oid` — the next (older) commit on the walk's first-parent
    /// chain — is pushed.
    fn check(&mut self, repo: &Repository, oid: Oid) -> bool {
        let Some(tip) = self.upstream_tip else {
            return false;
        };
        if self.chain_pushed {
            return true;
        }
        self.chain_pushed = oid == tip || repo.graph_descendant_of(tip, oid).unwrap_or(false);
        self.chain_pushed
    }
}

/// The files changed by `commit` vs its first parent (name-only), mirroring
/// `git show --name-only`. A root commit diffs against the empty tree.
fn changed_files(repo: &Repository, commit: &Commit) -> Result<Vec<String>> {
    let tree = commit.tree().map_err(map_git_err)?;
    let parent_tree = match commit.parent(0) {
        Ok(parent) => Some(parent.tree().map_err(map_git_err)?),
        Err(_) => None,
    };
    let diff = repo
        .diff_tree_to_tree(parent_tree.as_ref(), Some(&tree), None)
        .map_err(map_git_err)?;
    let mut files = Vec::new();
    for delta in diff.deltas() {
        let path = delta.new_file().path().or_else(|| delta.old_file().path());
        if let Some(p) = path {
            let s = p.to_string_lossy().to_string();
            if !files.contains(&s) {
                files.push(s);
            }
        }
    }
    Ok(files)
}

/// Per-file additions/deletions for a single commit, used by [`CommitDetails`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitFileChange {
    pub path: String,
    pub additions: usize,
    pub deletions: usize,
}

/// Full per-commit detail record: metadata plus the changed-file list with
/// per-file line stats. Returned by [`commit_details`] and projected by
/// `intent-services` onto the `git.commitDetails` wire shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitDetails {
    pub hash: String,
    pub author: String,
    pub author_email: String,
    pub date: String,
    pub message: String,
    pub files: Vec<CommitFileChange>,
}

#[allow(clippy::similar_names)] // path vs the libgit2 patch - both domain terms
/// Resolve `commit_hash` (full SHA or short ref) against `worktree_path` and
/// return its metadata plus the per-file `(additions, deletions)` diff against
/// the first parent. A root commit (no parent) diffs against the empty tree, so
/// every file appears as additions. An unresolvable hash returns
/// [`Error::NotFound`].
///
/// # Errors
///
/// Returns [`Error::NotFound`] if `commit_hash` does not resolve to a commit; `Error::Internal` if another libgit2 operation fails.
///
/// # Panics
///
/// Panics if a diff delta reported by libgit2 cannot be re-fetched by index (an internal libgit2 invariant violation).
pub fn commit_details(worktree_path: &Path, commit_hash: &str) -> Result<CommitDetails> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let obj = repo
        .revparse_single(commit_hash)
        .map_err(|_| Error::NotFound(format!("commit not found: {commit_hash}")))?;
    let commit = obj
        .peel_to_commit()
        .map_err(|_| Error::NotFound(format!("commit not found: {commit_hash}")))?;
    let author = commit.author();
    let tree = commit.tree().map_err(map_git_err)?;
    let parent_tree = match commit.parent(0) {
        Ok(parent) => Some(parent.tree().map_err(map_git_err)?),
        Err(_) => None,
    };
    let diff = repo
        .diff_tree_to_tree(parent_tree.as_ref(), Some(&tree), None)
        .map_err(map_git_err)?;
    let mut files = Vec::new();
    for i in 0..diff.deltas().len() {
        let delta = diff.get_delta(i).expect("delta index in range");
        let path = delta
            .new_file()
            .path()
            .or_else(|| delta.old_file().path())
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();
        if path.is_empty() {
            continue;
        }
        let (additions, deletions) = match Patch::from_diff(&diff, i).map_err(map_git_err)? {
            Some(patch) => {
                let (_ctx, adds, dels) = patch.line_stats().map_err(map_git_err)?;
                (adds, dels)
            }
            None => (0, 0),
        };
        files.push(CommitFileChange {
            path,
            additions,
            deletions,
        });
    }
    Ok(CommitDetails {
        hash: commit.id().to_string(),
        author: author.name().unwrap_or("").to_string(),
        author_email: author.email().unwrap_or("").to_string(),
        date: iso_from_unix_secs(commit.time().seconds()),
        message: commit.summary().ok().flatten().unwrap_or("").to_string(),
        files,
    })
}

/// Resolve the workspace boundary commit SHA for bounding history walks.
///
/// Implements the reference logic from `cloudlands-fe _doGetHistory`:
/// 1. Prefer merge-base of HEAD vs `origin/<base_ref>`, then `<base_ref>` (rebase-resilient).
/// 2. Fallback to `base_commit_sha` when it is a valid ancestor of HEAD.
/// 3. Return `None` when no boundary info or nothing resolves.
///
/// This is the boundary for `file-tracking.loadCommits` so the Changes panel
/// only shows workspace-owned commits (`boundary..HEAD`).
///
/// # Errors
///
/// Returns `Error::Internal` if the underlying libgit2 operation fails.
pub fn resolve_workspace_boundary(
    worktree_path: &Path,
    base_ref: Option<&str>,
    base_commit_sha: Option<&str>,
) -> Result<Option<String>> {
    let started = Instant::now();
    let out = resolve_workspace_boundary_inner(worktree_path, base_ref, base_commit_sha);
    if let Ok(boundary) = &out {
        tracing::debug!(
            resolved = boundary.is_some(),
            base_ref = base_ref.unwrap_or(""),
            total_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            "resolve_workspace_boundary"
        );
    }
    out
}

fn resolve_workspace_boundary_inner(
    worktree_path: &Path,
    base_ref: Option<&str>,
    base_commit_sha: Option<&str>,
) -> Result<Option<String>> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;

    // Get HEAD first - if unavailable, no boundary can be resolved
    let Some(head_oid) = repo.head().ok().and_then(|h| h.target()) else {
        // Detached/missing HEAD → no boundary
        return Ok(None);
    };

    // Try merge-base first (rebase-resilient)
    if let Some(base_ref) = base_ref {
        // Try origin/<base_ref> first, then <base_ref>
        for ref_name in [format!("origin/{base_ref}"), base_ref.to_string()] {
            if let Ok(obj) = repo.revparse_single(&ref_name) {
                if let Ok(base_oid) = repo.merge_base(head_oid, obj.id()) {
                    return Ok(Some(base_oid.to_string()));
                }
            }
        }
    }

    // Fallback: validate base_commit_sha as ancestor of HEAD
    if let Some(base_sha) = base_commit_sha {
        if let Ok(base_obj) = repo.revparse_single(base_sha) {
            // Check if base_sha is an ancestor of HEAD (using head_oid from above)
            if repo.merge_base(base_obj.id(), head_oid).ok() == Some(base_obj.id()) {
                return Ok(Some(base_obj.id().to_string()));
            }
        }
    }

    Ok(None)
}

/// Read history bounded by a workspace boundary (for `file-tracking.loadCommits`).
///
/// Returns commits in the range `boundary..HEAD` (workspace-owned only) when
/// `boundary_sha` is provided and valid. When `boundary_sha` is `None`, returns
/// unbounded history (the existing behavior). When `include_older` is true,
/// fetches commits **before and including** the boundary (powers the FE
/// "show previous" toggle; the boundary commit itself is included). With
/// `include_files = false` the per-commit tree diff is skipped entirely —
/// `files` is `None` — keeping the walk O(commits) cheap for list payloads;
/// clients fetch per-file data on demand via `git.commitDetails`.
///
/// # Errors
///
/// Returns `Error::Internal` if the revwalk fails or another libgit2 operation fails.
pub fn history_bounded(
    worktree_path: &Path,
    boundary_sha: Option<&str>,
    limit: usize,
    include_older: bool,
    include_files: bool,
) -> Result<Vec<CommitRecord>> {
    let started = Instant::now();
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    if repo.head().ok().and_then(|h| h.target()).is_none() {
        return Ok(Vec::new());
    }

    let branch = current_branch(&repo);
    let mut pushed = PushedCheck::new(upstream_tip(&repo, &branch));

    let mut walk = repo.revwalk().map_err(map_git_err)?;
    walk.push_head().map_err(map_git_err)?;

    if let Some(boundary) = boundary_sha {
        if let Ok(obj) = repo.revparse_single(boundary) {
            if include_older {
                // For "show previous": start FROM the boundary (inclusive) and walk backward
                walk.reset().map_err(map_git_err)?;
                walk.push(obj.id()).map_err(map_git_err)?;
            } else {
                // Normal case: hide the boundary so we get boundary..HEAD
                let _ = walk.hide(obj.id());
            }
        }
    }

    walk.simplify_first_parent().map_err(map_git_err)?;
    walk.set_sorting(Sort::TIME).map_err(map_git_err)?;

    let timing_enabled = tracing::enabled!(tracing::Level::DEBUG);
    let mut diff_elapsed = Duration::ZERO;
    let mut pushed_elapsed = Duration::ZERO;
    let mut out = Vec::new();
    for oid in walk {
        if out.len() >= limit {
            break;
        }
        let oid = oid.map_err(map_git_err)?;
        let commit = repo.find_commit(oid).map_err(map_git_err)?;
        // Skip merge commits
        if commit.parent_count() > 1 {
            continue;
        }
        let hash = oid.to_string();
        let pushed_started = timing_enabled.then(Instant::now);
        let is_pushed = pushed.check(&repo, oid);
        if let Some(t) = pushed_started {
            pushed_elapsed += t.elapsed();
        }
        let (agent_id, linked_note_id) = parse_trailers(commit.body().ok().flatten().unwrap_or(""));
        let files = if include_files {
            let diff_started = timing_enabled.then(Instant::now);
            let files = changed_files(&repo, &commit)?;
            if let Some(t) = diff_started {
                diff_elapsed += t.elapsed();
            }
            Some(files)
        } else {
            None
        };
        let files_changed = files.as_ref().map_or(0, Vec::len);
        let author = commit.author();
        out.push(CommitRecord {
            hash,
            message: commit.summary().ok().flatten().unwrap_or("").to_string(),
            author: author.name().unwrap_or("").to_string(),
            author_email: author.email().unwrap_or("").to_string(),
            date: iso_from_unix_secs(commit.time().seconds()),
            files,
            files_changed,
            is_pushed,
            agent_id,
            linked_note_id,
        });
    }
    let total = started.elapsed();
    tracing::debug!(
        commits = out.len(),
        limit,
        include_older,
        include_files,
        bounded = boundary_sha.is_some(),
        pushed_check_ms = u64::try_from(pushed_elapsed.as_millis()).unwrap_or(u64::MAX),
        per_commit_diff_ms = u64::try_from(diff_elapsed.as_millis()).unwrap_or(u64::MAX),
        other_ms = u64::try_from(
            total
                .saturating_sub(diff_elapsed)
                .saturating_sub(pushed_elapsed)
                .as_millis()
        )
        .unwrap_or(u64::MAX),
        total_ms = u64::try_from(total.as_millis()).unwrap_or(u64::MAX),
        "history_bounded: revwalk + per-commit tree diffs"
    );
    if total >= SLOW_GIT_WARN_THRESHOLD {
        tracing::warn!(
            worktree = %worktree_path.display(),
            limit,
            commits = out.len(),
            total_ms = u64::try_from(total.as_millis()).unwrap_or(u64::MAX),
            "history_bounded: slow history read"
        );
    }
    Ok(out)
}

/// Parse the `Agent-Id:` / `Linked-Note-Id:` trailers from a commit body,
/// mirroring the TS `loadCommits` trailer scan.
pub(crate) fn parse_trailers(body: &str) -> (Option<String>, Option<String>) {
    let mut agent_id = None;
    let mut linked_note_id = None;
    for line in body.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("Agent-Id:") {
            let v = rest.trim();
            if !v.is_empty() {
                agent_id = Some(v.to_string());
            }
        } else if let Some(rest) = line.strip_prefix("Linked-Note-Id:") {
            let v = rest.trim();
            if !v.is_empty() {
                linked_note_id = Some(v.to_string());
            }
        }
    }
    (agent_id, linked_note_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{commit_file, init_repo};
    use std::process::Command;

    #[test]
    fn empty_repo_has_no_history() {
        let dir = init_repo("history-empty");
        assert!(history(dir.path(), 50).unwrap().is_empty());
    }

    #[test]
    fn returns_commits_newest_first_with_files() {
        let dir = init_repo("history-basic");
        commit_file(dir.path(), "a.txt", "one\n");
        commit_file(dir.path(), "b.txt", "two\n");
        let commits = history(dir.path(), 50).unwrap();
        assert_eq!(commits.len(), 2);
        assert_eq!(
            commits[0].files.as_deref(),
            Some(&["b.txt".to_string()][..])
        );
        assert_eq!(commits[0].files_changed, 1);
        // No upstream → every commit is treated as unpushed.
        assert!(!commits[0].is_pushed);
        assert!(!commits[0].date.is_empty());
    }

    #[test]
    fn limit_caps_the_result() {
        let dir = init_repo("history-limit");
        commit_file(dir.path(), "a.txt", "1\n");
        commit_file(dir.path(), "b.txt", "2\n");
        commit_file(dir.path(), "c.txt", "3\n");
        let commits = history(dir.path(), 2).unwrap();
        assert_eq!(commits.len(), 2);
        assert_eq!(
            commits[0].files.as_deref(),
            Some(&["c.txt".to_string()][..])
        );
    }

    #[test]
    fn history_since_without_files_skips_tree_diffs() {
        let dir = init_repo("history-no-files");
        commit_file(dir.path(), "a.txt", "one\n");
        commit_file(dir.path(), "b.txt", "two\n");
        let commits = history_since(dir.path(), None, 50, false).unwrap();
        assert_eq!(commits.len(), 2);
        for c in &commits {
            assert_eq!(c.files, None);
            assert_eq!(c.files_changed, 0);
            assert!(!c.message.is_empty());
            assert!(!c.date.is_empty());
        }
    }

    /// Point `refs/remotes/origin/<branch>` (the current branch) at `target`.
    fn set_upstream_ref(worktree: &Path, target: &str) {
        let repo = Repository::open(worktree).unwrap();
        let branch = repo.head().unwrap().shorthand().unwrap().to_string();
        let oid = git2::Oid::from_str(target).unwrap();
        repo.reference(
            &format!("refs/remotes/origin/{branch}"),
            oid,
            true,
            "test upstream",
        )
        .unwrap();
    }

    /// Like `commit_file` but with an explicit commit timestamp, so `Sort::TIME`
    /// walks return a deterministic newest-first order (same-second commits tie).
    fn commit_file_at(worktree: &Path, rel: &str, contents: &str, unix_secs: i64) {
        crate::testutil::write_file(worktree, rel, contents);
        let repo = Repository::open(worktree).unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(Path::new(rel)).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = repo.find_tree(tree_oid).unwrap();
        let sig = git2::Signature::new("Test", "test@example.com", &git2::Time::new(unix_secs, 0))
            .unwrap();
        let parents = match repo.head().ok().and_then(|h| h.target()) {
            Some(oid) => vec![repo.find_commit(oid).unwrap()],
            None => Vec::new(),
        };
        let parent_refs: Vec<&git2::Commit> = parents.iter().collect();
        repo.commit(Some("HEAD"), &sig, &sig, "commit", &tree, &parent_refs)
            .unwrap();
    }

    const T0: i64 = 1_700_000_000;

    #[test]
    fn is_pushed_reflects_upstream_tip_and_ancestors() {
        let dir = init_repo("history-pushed");
        commit_file_at(dir.path(), "a.txt", "one\n", T0);
        commit_file_at(dir.path(), "b.txt", "two\n", T0 + 1);
        let pushed_tip = history(dir.path(), 50).unwrap()[0].hash.clone();
        set_upstream_ref(dir.path(), &pushed_tip);
        commit_file_at(dir.path(), "c.txt", "three\n", T0 + 2);

        let commits = history(dir.path(), 50).unwrap();
        assert_eq!(commits.len(), 3);
        // Newest commit is ahead of the upstream tip → unpushed.
        assert!(!commits[0].is_pushed);
        // The upstream tip itself and its ancestor are pushed.
        assert_eq!(commits[1].hash, pushed_tip);
        assert!(commits[1].is_pushed);
        assert!(commits[2].is_pushed);
    }

    #[test]
    fn is_pushed_diverged_upstream_marks_only_common_ancestors() {
        let dir = init_repo("history-diverged");
        commit_file_at(dir.path(), "a.txt", "base\n", T0);
        let default_branch = current_branch(&Repository::open(dir.path()).unwrap());
        // A side branch diverges from the base commit; its tip becomes the
        // upstream of the default branch.
        crate::testutil::create_branch(dir.path(), "side");
        crate::testutil::checkout_branch(dir.path(), "side");
        commit_file_at(dir.path(), "remote.txt", "remote-only\n", T0 + 1);
        let side_tip = history(dir.path(), 50).unwrap()[0].hash.clone();
        crate::testutil::checkout_branch(dir.path(), &default_branch);
        set_upstream_ref(dir.path(), &side_tip);
        commit_file_at(dir.path(), "local.txt", "local-only\n", T0 + 2);

        let commits = history(dir.path(), 50).unwrap();
        assert_eq!(commits.len(), 2);
        // Local-only commit is not an ancestor of the diverged upstream tip.
        assert!(!commits[0].is_pushed);
        // The common base is an ancestor of the upstream tip → pushed.
        assert!(commits[1].is_pushed);
    }

    #[test]
    fn large_divergence_page_has_correct_flags() {
        let dir = init_repo("history-large-divergence");
        commit_file_at(dir.path(), "base.txt", "base\n", T0);
        let pushed_tip = history(dir.path(), 50).unwrap()[0].hash.clone();
        set_upstream_ref(dir.path(), &pushed_tip);
        for i in 0..60 {
            commit_file_at(dir.path(), "churn.txt", &format!("rev {i}\n"), T0 + 1 + i);
        }

        let page = history_since(dir.path(), None, 10, false).unwrap();
        assert_eq!(page.len(), 10);
        assert!(page.iter().all(|c| !c.is_pushed));

        let all = history_since(dir.path(), None, 100, false).unwrap();
        assert_eq!(all.len(), 61);
        assert!(all[..60].iter().all(|c| !c.is_pushed));
        assert_eq!(all[60].hash, pushed_tip);
        assert!(all[60].is_pushed);
    }

    #[test]
    fn behind_upstream_all_page_commits_are_pushed() {
        let dir = init_repo("history-behind");
        for i in 0..5 {
            commit_file_at(dir.path(), "a.txt", &format!("rev {i}\n"), T0 + i);
        }
        let remote_tip = history(dir.path(), 50).unwrap()[0].hash.clone();
        set_upstream_ref(dir.path(), &remote_tip);
        // Rewind HEAD two commits so the branch is behind origin by 2
        // (fetch-without-pull): every local commit is an ancestor of the tip.
        let repo = Repository::open(dir.path()).unwrap();
        let target = repo.revparse_single("HEAD~2").unwrap();
        repo.reset(&target, git2::ResetType::Hard, None).unwrap();

        let commits = history(dir.path(), 50).unwrap();
        assert_eq!(commits.len(), 3);
        assert!(commits.iter().all(|c| c.is_pushed));
    }

    #[test]
    fn pushed_check_short_circuits_after_first_pushed_commit() {
        let dir = init_repo("pushed-check-short-circuit");
        commit_file_at(dir.path(), "a.txt", "one\n", T0);
        let repo = Repository::open(dir.path()).unwrap();
        let tip = repo.head().unwrap().target().unwrap();
        // An oid the ancestry query can never resolve: it checks as unpushed
        // unless the chain already short-circuited.
        let bogus = Oid::from_str("deadbeefdeadbeefdeadbeefdeadbeefdeadbeef").unwrap();

        let mut fresh = PushedCheck::new(Some(tip));
        assert!(!fresh.check(&repo, bogus));

        // Once a commit on the chain checks as pushed, subsequent (older)
        // commits are marked pushed without an ancestry query.
        let mut chain = PushedCheck::new(Some(tip));
        assert!(chain.check(&repo, tip));
        assert!(chain.check(&repo, bogus));

        // No upstream never reports pushed, short-circuit or not.
        let mut none = PushedCheck::new(None);
        assert!(!none.check(&repo, tip));
        assert!(!none.check(&repo, tip));
    }

    #[test]
    fn parses_attribution_trailers_from_body() {
        let (agent, note) =
            parse_trailers("Subject\n\nAgent-Id: agent-123\nLinked-Note-Id: note-9\n");
        assert_eq!(agent.as_deref(), Some("agent-123"));
        assert_eq!(note.as_deref(), Some("note-9"));
        let (none_a, none_n) = parse_trailers("no trailers here");
        assert!(none_a.is_none());
        assert!(none_n.is_none());
    }

    #[test]
    fn commit_details_returns_metadata_and_file_stats() {
        let dir = init_repo("commit-details-basic");
        commit_file(dir.path(), "a.txt", "line1\nline2\nline3\n");
        commit_file(dir.path(), "a.txt", "line1\nCHANGED\nline3\nline4\n");
        let commits = history(dir.path(), 50).unwrap();
        let head = &commits[0];
        let details = commit_details(dir.path(), &head.hash).unwrap();
        assert_eq!(details.hash, head.hash);
        assert_eq!(details.author, "Test");
        assert_eq!(details.author_email, "test@example.com");
        assert!(!details.date.is_empty());
        assert_eq!(details.files.len(), 1);
        let f = &details.files[0];
        assert_eq!(f.path, "a.txt");
        // One line replaced (1 add + 1 del) + one appended (1 add).
        assert_eq!(f.additions, 2);
        assert_eq!(f.deletions, 1);
    }

    #[test]
    fn commit_details_root_commit_is_all_additions() {
        let dir = init_repo("commit-details-root");
        commit_file(dir.path(), "seed.txt", "one\ntwo\n");
        let commits = history(dir.path(), 50).unwrap();
        let root = &commits[0];
        let details = commit_details(dir.path(), &root.hash).unwrap();
        assert_eq!(details.files.len(), 1);
        assert_eq!(details.files[0].path, "seed.txt");
        assert_eq!(details.files[0].additions, 2);
        assert_eq!(details.files[0].deletions, 0);
    }

    #[test]
    fn commit_details_unknown_hash_is_not_found() {
        let dir = init_repo("commit-details-missing");
        commit_file(dir.path(), "a.txt", "seed\n");
        let err =
            commit_details(dir.path(), "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef").unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[test]
    fn resolve_workspace_boundary_no_info_returns_none() {
        let dir = init_repo("boundary-no-info");
        commit_file(dir.path(), "a.txt", "one\n");
        let boundary = resolve_workspace_boundary(dir.path(), None, None).unwrap();
        assert!(boundary.is_none());
    }

    #[test]
    fn resolve_workspace_boundary_uses_merge_base_with_ref() {
        let dir = init_repo("boundary-merge-base");
        commit_file(dir.path(), "a.txt", "base\n");
        let all = history(dir.path(), 50).unwrap();
        let base_sha = &all[0].hash;

        // Create a main branch at the base commit
        Command::new("git")
            .current_dir(dir.path())
            .args(["branch", "main"])
            .output()
            .unwrap();

        // Create a feature branch and add commits
        Command::new("git")
            .current_dir(dir.path())
            .args(["checkout", "-b", "feat/test"])
            .output()
            .unwrap();
        commit_file(dir.path(), "b.txt", "workspace\n");

        // resolve_workspace_boundary should use merge-base with main
        let boundary = resolve_workspace_boundary(dir.path(), Some("main"), None).unwrap();
        assert_eq!(boundary.as_deref(), Some(base_sha.as_str()));
    }

    #[test]
    fn resolve_workspace_boundary_uses_base_commit_sha_fallback() {
        let dir = init_repo("boundary-sha-fallback");
        commit_file(dir.path(), "a.txt", "base\n");
        let base_commits = history(dir.path(), 50).unwrap();
        let base_sha = &base_commits[0].hash;
        commit_file(dir.path(), "b.txt", "workspace\n");

        let boundary = resolve_workspace_boundary(dir.path(), None, Some(base_sha)).unwrap();
        assert_eq!(boundary.as_deref(), Some(base_sha.as_str()));
    }

    #[test]
    fn resolve_workspace_boundary_rejects_non_ancestor_sha() {
        let dir = init_repo("boundary-non-ancestor");
        commit_file(dir.path(), "a.txt", "one\n");
        let fake_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        let boundary = resolve_workspace_boundary(dir.path(), None, Some(fake_sha)).unwrap();
        assert!(boundary.is_none());
    }

    #[test]
    fn history_bounded_returns_commits_after_boundary() {
        let dir = init_repo("history-bounded-basic");
        commit_file(dir.path(), "a.txt", "base\n");
        let all = history(dir.path(), 50).unwrap();
        let boundary_sha = &all[0].hash;

        commit_file(dir.path(), "b.txt", "workspace-1\n");
        commit_file(dir.path(), "c.txt", "workspace-2\n");

        let bounded = history_bounded(dir.path(), Some(boundary_sha), 50, false, true).unwrap();
        assert_eq!(bounded.len(), 2);
        assert_eq!(
            bounded[0].files.as_deref(),
            Some(&["c.txt".to_string()][..])
        );
        assert_eq!(
            bounded[1].files.as_deref(),
            Some(&["b.txt".to_string()][..])
        );
    }

    #[test]
    fn history_bounded_at_head_returns_empty() {
        let dir = init_repo("history-bounded-at-head");
        commit_file(dir.path(), "a.txt", "one\n");
        let commits = history(dir.path(), 50).unwrap();
        let head_sha = &commits[0].hash;

        let bounded = history_bounded(dir.path(), Some(head_sha), 50, false, true).unwrap();
        assert!(bounded.is_empty());
    }

    #[test]
    fn history_bounded_no_boundary_returns_all() {
        let dir = init_repo("history-bounded-unbounded");
        commit_file(dir.path(), "a.txt", "one\n");
        commit_file(dir.path(), "b.txt", "two\n");

        let bounded = history_bounded(dir.path(), None, 50, false, true).unwrap();
        assert_eq!(bounded.len(), 2);
    }

    #[test]
    fn history_bounded_without_files_skips_tree_diffs() {
        let dir = init_repo("history-bounded-no-files");
        commit_file(dir.path(), "a.txt", "one\n");
        commit_file(dir.path(), "b.txt", "two\n");

        let bounded = history_bounded(dir.path(), None, 50, false, false).unwrap();
        assert_eq!(bounded.len(), 2);
        for c in &bounded {
            assert_eq!(c.files, None);
            assert_eq!(c.files_changed, 0);
            assert!(!c.message.is_empty());
            assert!(!c.date.is_empty());
        }
    }

    #[test]
    fn history_bounded_include_older_fetches_before_boundary() {
        let dir = init_repo("history-bounded-older");
        commit_file(dir.path(), "a.txt", "old-1\n");
        commit_file(dir.path(), "b.txt", "old-2\n");
        let all = history(dir.path(), 50).unwrap();
        let boundary_sha = &all[0].hash; // boundary at "old-2"

        commit_file(dir.path(), "c.txt", "workspace\n");

        // Normal bounded: should get workspace commits only
        let bounded = history_bounded(dir.path(), Some(boundary_sha), 50, false, true).unwrap();
        assert_eq!(bounded.len(), 1);
        assert_eq!(
            bounded[0].files.as_deref(),
            Some(&["c.txt".to_string()][..])
        );

        // Include older: should get commits before boundary
        let older = history_bounded(dir.path(), Some(boundary_sha), 50, true, true).unwrap();
        assert_eq!(older.len(), 2);
        assert_eq!(older[0].files.as_deref(), Some(&["b.txt".to_string()][..]));
        assert_eq!(older[1].files.as_deref(), Some(&["a.txt".to_string()][..]));
    }
}
