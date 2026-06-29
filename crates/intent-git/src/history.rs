//! Commit history (`file-tracking.loadCommits`).
//!
//! Ports the `git log --first-parent --no-merges -n <limit>` read the TS
//! file-tracking `loadCommits` handler performs: the linear first-parent history
//! from `HEAD`, with each commit carrying its changed-file list, push state
//! (against `origin/<branch>`), and the agent-attribution trailers
//! (`Agent-Id:` / `Linked-Note-Id:`) parsed from the commit body. Returned to the
//! wire as `CommitWithAttribution[]` by `intent-services` (M4.8).

use std::collections::HashSet;
use std::path::Path;

use git2::{Commit, Repository, Sort};
use intent_core::{iso_from_unix_secs, Result};

use crate::map_git_err;
use crate::status::current_branch;

/// One commit's history record, the BE shape behind `CommitWithAttribution`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitRecord {
    pub hash: String,
    pub message: String,
    pub author: String,
    pub author_email: String,
    pub date: String,
    pub files: Vec<String>,
    pub files_changed: usize,
    pub is_pushed: bool,
    pub agent_id: Option<String>,
    pub linked_note_id: Option<String>,
}

/// Read up to `limit` commits of first-parent, non-merge history from `HEAD`,
/// newest first. An empty repository (unborn `HEAD`) yields an empty list.
pub fn history(worktree_path: &Path, limit: usize) -> Result<Vec<CommitRecord>> {
    history_since(worktree_path, None, limit)
}

/// Like [`history`] but, when `base_ref` resolves, hides it from the walk so the
/// result is the `base_ref..HEAD` range (the accept-changes `localCommits`:
/// commits on the branch not yet on trunk). An unresolvable `base_ref` falls back
/// to the full `HEAD` history.
pub fn history_since(
    worktree_path: &Path,
    base_ref: Option<&str>,
    limit: usize,
) -> Result<Vec<CommitRecord>> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    if repo.head().ok().and_then(|h| h.target()).is_none() {
        return Ok(Vec::new());
    }

    let branch = current_branch(&repo);
    let (unpushed, has_upstream) = unpushed_hashes(&repo, &branch);

    let mut walk = repo.revwalk().map_err(map_git_err)?;
    walk.push_head().map_err(map_git_err)?;
    if let Some(base) = base_ref {
        if let Ok(obj) = repo.revparse_single(base) {
            let _ = walk.hide(obj.id());
        }
    }
    walk.simplify_first_parent().map_err(map_git_err)?;
    walk.set_sorting(Sort::TIME).map_err(map_git_err)?;

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
        let is_pushed = has_upstream && !unpushed.contains(&hash);
        let (agent_id, linked_note_id) = parse_trailers(commit.body().ok().flatten().unwrap_or(""));
        let files = changed_files(&repo, &commit)?;
        let files_changed = files.len();
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
    Ok(out)
}

/// The set of commit hashes in `origin/<branch>..HEAD` (the unpushed commits),
/// plus whether the upstream ref exists. With no upstream every commit is
/// treated as unpushed (the TS `hasUpstream` fallback).
fn unpushed_hashes(repo: &Repository, branch: &str) -> (HashSet<String>, bool) {
    let empty = (HashSet::new(), false);
    if branch.is_empty() {
        return empty;
    }
    let upstream = format!("refs/remotes/origin/{branch}");
    let Some(upstream_oid) = repo.find_reference(&upstream).ok().and_then(|r| r.target()) else {
        return empty;
    };
    let Ok(mut walk) = repo.revwalk() else {
        return empty;
    };
    if walk.push_head().is_err() || walk.hide(upstream_oid).is_err() {
        return empty;
    }
    let mut set = HashSet::new();
    for oid in walk {
        match oid {
            Ok(oid) => {
                set.insert(oid.to_string());
            }
            Err(_) => return empty,
        }
    }
    (set, true)
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
        assert_eq!(commits[0].files, vec!["b.txt".to_string()]);
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
        assert_eq!(commits[0].files, vec!["c.txt".to_string()]);
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
}
