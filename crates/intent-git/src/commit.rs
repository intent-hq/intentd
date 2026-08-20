//! Commit creation (`git.commit` / `git.agentCommit`).
//!
//! Ports `gitService.commit`: a commit is built from the current index
//! (already-staged changes) using the repository's configured identity, mirroring
//! `git commit -m <message>`. An empty commit (nothing staged) is rejected like
//! the TS "nothing to commit" path. The `agentCommit` auto-stage step computes
//! the set of changed paths via [`all_changed_paths`] (see the parity note in
//! `intent-services`: without the file-tracking attribution pipeline the set is
//! the whole worktree's changes rather than a single agent's).
//!
//! Note: libgit2 does not run git hooks, so the TS pre-commit-hook retry loop has
//! no analogue here.

use std::path::Path;

use git2::{Commit, Repository, Tree};
use intent_core::{Error, Result};

use crate::map_git_err;
use crate::submodule::reject_submodule_internal_paths;

/// Error message returned when the staged tree matches the parent commit exactly
/// (nothing to commit). Public so auto-commit can recognize this benign skip condition.
pub const CLEAN_TREE_ERROR: &str = "nothing to commit, working tree clean";

/// The outcome of creating a commit: the new commit SHA and the files it changed.
#[derive(Debug, Clone)]
pub struct CommitOutcome {
    pub hash: String,
    pub files: Vec<String>,
}

/// Create a commit from the current index (already-staged changes), mirroring
/// `git commit -m <message>`. Errors when there is nothing staged to commit.
pub fn commit(worktree_path: &Path, message: &str) -> Result<CommitOutcome> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let mut index = repo.index().map_err(map_git_err)?;
    let tree_oid = index.write_tree().map_err(map_git_err)?;
    let tree = repo.find_tree(tree_oid).map_err(map_git_err)?;

    let parent = repo
        .head()
        .ok()
        .and_then(|h| h.target())
        .and_then(|oid| repo.find_commit(oid).ok());

    // Reject an empty commit, mirroring the TS "nothing to commit" failure.
    if let Some(parent) = &parent {
        if parent.tree_id() == tree_oid {
            return Err(Error::Internal(CLEAN_TREE_ERROR.to_string()));
        }
    }

    let sig = repo.signature().map_err(map_git_err)?;
    let parents: Vec<&Commit> = parent.iter().collect();
    let oid = repo
        .commit(Some("HEAD"), &sig, &sig, message, &tree, &parents)
        .map_err(map_git_err)?;

    let files = changed_files(&repo, parent.as_ref(), &tree)?;
    Ok(CommitOutcome {
        hash: oid.to_string(),
        files,
    })
}

/// Create a commit whose body carries attribution trailers, building the message
/// via [`build_commit_message`] before committing the staged index. Mirrors
/// [`commit`] except for the trailer-aware message; used by the agent commit path
/// while bare [`commit`] backs `git.commit`.
pub fn commit_with_trailers(
    worktree_path: &Path,
    message: &str,
    agent_id: Option<&str>,
    linked_note_id: Option<&str>,
) -> Result<CommitOutcome> {
    let body = build_commit_message(message, agent_id, linked_note_id);
    commit(worktree_path, &body)
}

/// Create a commit whose tree is the parent tree plus **only** the given
/// `paths` taken from the working tree (`git commit -- <paths>` semantics),
/// with attribution trailers. Pre-staged changes to other paths are left in
/// the on-disk index and are never swept into the commit — used by the
/// attribution-filtered `git.agentCommit` fallback so another actor's staged
/// work cannot ride along. `paths` are worktree-relative; a path missing from
/// the working tree is committed as a deletion.
pub fn commit_paths_with_trailers(
    worktree_path: &Path,
    message: &str,
    agent_id: Option<&str>,
    linked_note_id: Option<&str>,
    paths: &[String],
) -> Result<CommitOutcome> {
    let body = build_commit_message(message, agent_id, linked_note_id);
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    // Refuse any path strictly inside a registered submodule BEFORE any index
    // mutation (monorepo#1714): a rejected batch leaves the in-memory tree
    // build below untouched, and since the post-commit index refresh loop
    // iterates the same `paths`, it can never re-add a submodule-internal
    // path either. The gitlink path itself (a pin bump) is unaffected.
    reject_submodule_internal_paths(&repo, paths)?;
    let workdir = repo
        .workdir()
        .ok_or_else(|| Error::Internal("Repository has no working directory".to_string()))?
        .to_path_buf();
    let parent = repo
        .head()
        .ok()
        .and_then(|h| h.target())
        .and_then(|oid| repo.find_commit(oid).ok());

    // Build the commit tree in memory: seed from the parent tree, then apply
    // only `paths` from the working tree. This in-memory state is never
    // written back — the on-disk index (including other actors' staged
    // entries) is reloaded and updated per-path after the commit below.
    let mut index = repo.index().map_err(map_git_err)?;
    match &parent {
        Some(c) => {
            let parent_tree = c.tree().map_err(map_git_err)?;
            index.read_tree(&parent_tree).map_err(map_git_err)?;
        }
        None => index.clear().map_err(map_git_err)?,
    }
    for raw in paths {
        let rel = Path::new(raw);
        if workdir.join(rel).exists() {
            index.add_path(rel).map_err(map_git_err)?;
        } else if index.get_path(rel, 0).is_some() {
            // Tracked in the parent tree but deleted in the worktree → drop
            // from the commit tree.
            index.remove_path(rel).map_err(map_git_err)?;
        } else {
            // Neither on disk nor tracked: reject like `stage()` does so a
            // bogus explicit `files` entry still surfaces as an error.
            return Err(Error::Internal(format!(
                "fatal: pathspec '{raw}' did not match any files"
            )));
        }
    }
    let tree_oid = index.write_tree().map_err(map_git_err)?;
    let tree = repo.find_tree(tree_oid).map_err(map_git_err)?;

    if let Some(parent) = &parent {
        if parent.tree_id() == tree_oid {
            return Err(Error::Internal(CLEAN_TREE_ERROR.to_string()));
        }
    }

    let sig = repo.signature().map_err(map_git_err)?;
    let parents: Vec<&Commit> = parent.iter().collect();
    let oid = repo
        .commit(Some("HEAD"), &sig, &sig, &body, &tree, &parents)
        .map_err(map_git_err)?;

    // Refresh the on-disk index entries for the committed paths (matching
    // `git commit -- <paths>`, which updates the index for the pathspec) so
    // they read as clean against the new HEAD. `read(true)` drops the
    // in-memory tree built above and reloads the real index first, keeping
    // every other entry (including other actors' staged work) intact.
    index.read(true).map_err(map_git_err)?;
    for raw in paths {
        let rel = Path::new(raw);
        if workdir.join(rel).exists() {
            index.add_path(rel).map_err(map_git_err)?;
        } else {
            index.remove_path(rel).map_err(map_git_err)?;
        }
    }
    index.write().map_err(map_git_err)?;

    let files = changed_files(&repo, parent.as_ref(), &tree)?;
    Ok(CommitOutcome {
        hash: oid.to_string(),
        files,
    })
}

/// Build a commit message body with attribution trailers, porting the
/// reference `agent-commit.service.ts::buildCommitMessage`.
///
/// The message is trimmed and any run of 3+ consecutive newlines is collapsed to
/// a single blank line (matching `/\n{3,}/g → \n\n`). An `Agent-Id:` trailer is
/// appended when `agent_id` is present, followed by a `Linked-Note-Id:` trailer
/// when `linked_note_id` is present; the trailer block is separated from the body
/// by a single blank line. When neither id is given the cleaned message is
/// returned unchanged (no trailing blank line). The output round-trips with
/// [`crate::history::parse_trailers`].
pub(crate) fn build_commit_message(
    message: &str,
    agent_id: Option<&str>,
    linked_note_id: Option<&str>,
) -> String {
    let clean = collapse_blank_lines(message.trim());
    let mut trailers = Vec::new();
    if let Some(id) = agent_id {
        trailers.push(format!("Agent-Id: {id}"));
    }
    if let Some(id) = linked_note_id {
        trailers.push(format!("Linked-Note-Id: {id}"));
    }
    if trailers.is_empty() {
        clean
    } else {
        format!("{clean}\n\n{}", trailers.join("\n"))
    }
}

/// Collapse any run of 3+ consecutive `\n` into exactly two, mirroring the
/// reference `message.replace(/\n{3,}/g, '\n\n')`.
fn collapse_blank_lines(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut run = 0usize;
    for ch in s.chars() {
        if ch == '\n' {
            run += 1;
            if run <= 2 {
                out.push('\n');
            }
        } else {
            run = 0;
            out.push(ch);
        }
    }
    out
}

/// All distinct paths with worktree changes (staged, unstaged, or untracked),
/// the auto-stage set for `agentCommit` in the absence of agent attribution.
pub fn all_changed_paths(worktree_path: &Path) -> Result<Vec<String>> {
    let st = crate::status::status(worktree_path)?;
    let mut paths = Vec::new();
    for f in st.files {
        if !paths.contains(&f.path) {
            paths.push(f.path);
        }
    }
    Ok(paths)
}

/// All distinct paths with **index (staged)** changes only — the commit set
/// for a `userRequested` `agentCommit` given no explicit `files`: a user
/// checkpoint commits what the user staged (plain `git commit` semantics)
/// instead of sweeping every change in the worktree into the commit.
pub fn staged_paths(worktree_path: &Path) -> Result<Vec<String>> {
    let st = crate::status::status(worktree_path)?;
    let mut paths = Vec::new();
    for f in st.files {
        if f.staged && !paths.contains(&f.path) {
            paths.push(f.path);
        }
    }
    Ok(paths)
}

/// The files changed between `parent`'s tree and `new_tree` (the committed delta),
/// mirroring `git diff-tree --no-commit-id --name-only -r <hash>`.
fn changed_files(
    repo: &Repository,
    parent: Option<&Commit>,
    new_tree: &Tree,
) -> Result<Vec<String>> {
    let parent_tree = match parent {
        Some(c) => Some(c.tree().map_err(map_git_err)?),
        None => None,
    };
    let diff = repo
        .diff_tree_to_tree(parent_tree.as_ref(), Some(new_tree), None)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stage::stage;
    use crate::testutil::{commit_file, init_repo, write_file};

    #[test]
    fn commits_staged_changes_and_reports_files() {
        let dir = init_repo("commit-basic");
        commit_file(dir.path(), "seed.txt", "seed\n");
        write_file(dir.path(), "a.txt", "hi\n");
        stage(dir.path(), &["a.txt".to_string()]).unwrap();
        let out = commit(dir.path(), "add a").unwrap();
        assert_eq!(out.hash.len(), 40);
        assert_eq!(out.files, vec!["a.txt".to_string()]);
    }

    #[test]
    fn empty_commit_is_rejected() {
        let dir = init_repo("commit-empty");
        commit_file(dir.path(), "seed.txt", "seed\n");
        let err = commit(dir.path(), "nothing").unwrap_err();
        assert!(format!("{err}").contains("nothing to commit"));
    }

    #[test]
    fn build_commit_message_agent_id_only() {
        let msg = build_commit_message("Fix bug", Some("agent-123"), None);
        assert_eq!(msg, "Fix bug\n\nAgent-Id: agent-123");
    }

    #[test]
    fn build_commit_message_agent_id_and_linked_note_id() {
        let msg = build_commit_message("Fix bug", Some("agent-123"), Some("note-9"));
        assert_eq!(
            msg,
            "Fix bug\n\nAgent-Id: agent-123\nLinked-Note-Id: note-9"
        );
    }

    #[test]
    fn build_commit_message_no_trailers_returns_cleaned_message() {
        let msg = build_commit_message("  Fix bug  ", None, None);
        assert_eq!(msg, "Fix bug");
        assert!(!msg.ends_with('\n'));
    }

    #[test]
    fn build_commit_message_collapses_excess_blank_lines() {
        let msg = build_commit_message("Subject\n\n\n\nBody", Some("agent-1"), None);
        assert_eq!(msg, "Subject\n\nBody\n\nAgent-Id: agent-1");
    }

    #[test]
    fn build_commit_message_round_trips_via_parse_trailers() {
        let msg = build_commit_message("Subject", Some("agent-123"), Some("note-9"));
        let (agent, note) = crate::history::parse_trailers(&msg);
        assert_eq!(agent.as_deref(), Some("agent-123"));
        assert_eq!(note.as_deref(), Some("note-9"));

        let agent_only = build_commit_message("Subject", Some("agent-42"), None);
        let (agent, note) = crate::history::parse_trailers(&agent_only);
        assert_eq!(agent.as_deref(), Some("agent-42"));
        assert!(note.is_none());
    }

    #[test]
    fn commit_paths_leaves_unrelated_staged_entries_out_of_the_commit() {
        // Another actor pre-staged `staged.txt`; committing only `mine.txt`
        // must not sweep it in, and it must stay staged afterwards.
        let dir = init_repo("commit-paths-staged");
        commit_file(dir.path(), "seed.txt", "seed\n");
        write_file(dir.path(), "staged.txt", "other actor\n");
        stage(dir.path(), &["staged.txt".to_string()]).unwrap();
        write_file(dir.path(), "mine.txt", "mine\n");

        let out = commit_paths_with_trailers(
            dir.path(),
            "scoped",
            Some("agent-1"),
            None,
            &["mine.txt".to_string()],
        )
        .unwrap();
        assert_eq!(out.files, vec!["mine.txt".to_string()]);

        let st = crate::status::status(dir.path()).unwrap();
        let staged_entry = st
            .files
            .iter()
            .find(|f| f.path == "staged.txt")
            .expect("staged.txt still pending");
        assert!(staged_entry.staged, "staged.txt still staged: {st:?}");
        assert!(
            st.files.iter().all(|f| f.path != "mine.txt"),
            "mine.txt clean after commit: {st:?}"
        );
    }

    #[test]
    fn commit_paths_commits_worktree_deletion() {
        let dir = init_repo("commit-paths-delete");
        commit_file(dir.path(), "seed.txt", "seed\n");
        commit_file(dir.path(), "gone.txt", "bye\n");
        std::fs::remove_file(dir.path().join("gone.txt")).unwrap();

        let out = commit_paths_with_trailers(
            dir.path(),
            "remove gone",
            Some("agent-1"),
            None,
            &["gone.txt".to_string()],
        )
        .unwrap();
        assert_eq!(out.files, vec!["gone.txt".to_string()]);
        let st = crate::status::status(dir.path()).unwrap();
        assert!(
            st.files.iter().all(|f| f.path != "gone.txt"),
            "deletion committed: {st:?}"
        );
    }

    #[test]
    fn commit_paths_rejects_submodule_internal_path() {
        use crate::testutil::add_submodule;
        let child = init_repo("commit-sub-child");
        commit_file(child.path(), "a.txt", "a\n");
        let dir = init_repo("commit-sub-parent");
        commit_file(dir.path(), "seed.txt", "seed\n");
        add_submodule(dir.path(), child.path(), "sub");
        let head_before = crate::history::history(dir.path(), 1).unwrap()[0]
            .hash
            .clone();

        write_file(&dir.path().join("sub"), "a.txt", "changed\n");
        let err = commit_paths_with_trailers(
            dir.path(),
            "flatten sub",
            Some("agent-1"),
            None,
            &["sub/a.txt".to_string()],
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("is in submodule"),
            "unexpected error: {err}"
        );

        // HEAD did not advance, and the gitlink + on-disk index are intact.
        let head_after = crate::history::history(dir.path(), 1).unwrap()[0]
            .hash
            .clone();
        assert_eq!(head_before, head_after, "no commit must have been made");
        let st = crate::status::status(dir.path()).unwrap();
        assert!(
            st.files.iter().all(|f| f.path != "sub/a.txt"),
            "submodule-internal path must not be staged/committed: {st:?}"
        );
    }

    /// Regression companion to `stage_rejects_absolute_submodule_internal_path`:
    /// the guard matches on the repo-relative form, so an in-worktree absolute
    /// path to a submodule-internal file is refused before any tree build and
    /// HEAD/gitlink/index stay intact.
    #[test]
    fn commit_paths_rejects_absolute_submodule_internal_path() {
        use crate::testutil::add_submodule;
        let child = init_repo("commit-sub-child-abs");
        commit_file(child.path(), "a.txt", "a\n");
        let dir = init_repo("commit-sub-parent-abs");
        commit_file(dir.path(), "seed.txt", "seed\n");
        add_submodule(dir.path(), child.path(), "sub");
        let head_before = crate::history::history(dir.path(), 1).unwrap()[0]
            .hash
            .clone();

        write_file(&dir.path().join("sub"), "a.txt", "changed\n");
        let abs = dir.path().join("sub").join("a.txt");
        let err = commit_paths_with_trailers(
            dir.path(),
            "flatten sub via absolute path",
            Some("agent-1"),
            None,
            &[abs.to_string_lossy().to_string()],
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("is in submodule"),
            "unexpected error: {err}"
        );

        let head_after = crate::history::history(dir.path(), 1).unwrap()[0]
            .hash
            .clone();
        assert_eq!(head_before, head_after, "no commit must have been made");
        let repo = Repository::open(dir.path()).unwrap();
        let index = repo.index().unwrap();
        assert!(
            index
                .iter()
                .all(|e| !String::from_utf8_lossy(&e.path).starts_with("sub/")),
            "no submodule-internal blob may be in the index"
        );
        assert_eq!(
            index.get_path(Path::new("sub"), 0).unwrap().mode,
            u32::from(git2::FileMode::Commit),
            "gitlink entry intact"
        );
    }

    /// Regression (`./` bypass): a leading `./` is lexical noise the guard's
    /// component-wise match used to miss, so the same submodule-internal path
    /// spelled `./sub/a.txt` slipped through to the tree build.
    #[test]
    fn commit_paths_rejects_dot_slash_submodule_internal_path() {
        use crate::testutil::add_submodule;
        let child = init_repo("commit-sub-child-dotslash");
        commit_file(child.path(), "a.txt", "a\n");
        let dir = init_repo("commit-sub-parent-dotslash");
        commit_file(dir.path(), "seed.txt", "seed\n");
        add_submodule(dir.path(), child.path(), "sub");
        let head_before = crate::history::history(dir.path(), 1).unwrap()[0]
            .hash
            .clone();

        write_file(&dir.path().join("sub"), "a.txt", "changed\n");
        for spelling in ["./sub/a.txt", "sub/./a.txt", "sub//a.txt"] {
            let err = commit_paths_with_trailers(
                dir.path(),
                "flatten sub via ./ spelling",
                Some("agent-1"),
                None,
                &[spelling.to_string()],
            )
            .unwrap_err();
            assert!(
                format!("{err}").contains("is in submodule"),
                "unexpected error for {spelling}: {err}"
            );
        }

        let head_after = crate::history::history(dir.path(), 1).unwrap()[0]
            .hash
            .clone();
        assert_eq!(head_before, head_after, "no commit must have been made");
        assert_eq!(
            std::fs::read_to_string(dir.path().join("sub").join("a.txt")).unwrap(),
            "changed\n",
            "submodule working-copy file must survive"
        );
        let repo = Repository::open(dir.path()).unwrap();
        let index = repo.index().unwrap();
        assert!(
            index
                .iter()
                .all(|e| !String::from_utf8_lossy(&e.path).starts_with("sub/")),
            "no submodule-internal blob may be in the index"
        );
        assert_eq!(
            index.get_path(Path::new("sub"), 0).unwrap().mode,
            u32::from(git2::FileMode::Commit),
            "gitlink entry intact"
        );
    }

    #[test]
    fn commit_paths_gitlink_pin_bump_succeeds() {
        use crate::testutil::add_submodule;
        let child = init_repo("commit-sub-child-pin");
        commit_file(child.path(), "a.txt", "a\n");
        let dir = init_repo("commit-sub-parent-pin");
        commit_file(dir.path(), "seed.txt", "seed\n");
        add_submodule(dir.path(), child.path(), "sub");
        // Advance the submodule's checked-out commit so the gitlink in the
        // parent's worktree/index differs from HEAD's recorded gitlink.
        commit_file(&dir.path().join("sub"), "b.txt", "b\n");

        let out = commit_paths_with_trailers(
            dir.path(),
            "bump sub pin",
            Some("agent-1"),
            None,
            &["sub".to_string()],
        )
        .unwrap();
        assert_eq!(out.files, vec!["sub".to_string()]);
    }

    #[test]
    fn commit_paths_mixed_list_with_submodule_internal_errors_without_committing() {
        use crate::testutil::add_submodule;
        let child = init_repo("commit-sub-child-mixed");
        commit_file(child.path(), "a.txt", "a\n");
        let dir = init_repo("commit-sub-parent-mixed");
        commit_file(dir.path(), "seed.txt", "seed\n");
        add_submodule(dir.path(), child.path(), "sub");
        let head_before = crate::history::history(dir.path(), 1).unwrap()[0]
            .hash
            .clone();

        write_file(dir.path(), "normal.txt", "normal\n");
        write_file(&dir.path().join("sub"), "a.txt", "changed\n");
        let err = commit_paths_with_trailers(
            dir.path(),
            "mixed",
            Some("agent-1"),
            None,
            &["normal.txt".to_string(), "sub/a.txt".to_string()],
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("is in submodule"),
            "unexpected error: {err}"
        );

        let head_after = crate::history::history(dir.path(), 1).unwrap()[0]
            .hash
            .clone();
        assert_eq!(head_before, head_after, "no commit must have been made");
        // `normal.txt` must not have been committed or staged either — the
        // whole batch is rejected atomically, before any index mutation.
        let st = crate::status::status(dir.path()).unwrap();
        let normal = st
            .files
            .iter()
            .find(|f| f.path == "normal.txt")
            .expect("normal.txt still present as an uncommitted change");
        assert!(
            !normal.staged,
            "normal.txt must not have been staged either: {st:?}"
        );
    }

    #[test]
    fn commit_paths_rejects_unknown_pathspec() {
        let dir = init_repo("commit-paths-bogus");
        commit_file(dir.path(), "seed.txt", "seed\n");
        let err = commit_paths_with_trailers(
            dir.path(),
            "bogus",
            Some("agent-1"),
            None,
            &["nope.txt".to_string()],
        )
        .unwrap_err();
        assert!(format!("{err}").contains("did not match any files"));
    }

    #[test]
    fn commit_with_trailers_body_parses_back_via_history() {
        let dir = init_repo("commit-trailers");
        commit_file(dir.path(), "seed.txt", "seed\n");
        write_file(dir.path(), "a.txt", "hi\n");
        stage(dir.path(), &["a.txt".to_string()]).unwrap();
        commit_with_trailers(dir.path(), "Add a", Some("agent-77"), None).unwrap();
        let commits = crate::history::history(dir.path(), 1).unwrap();
        assert_eq!(commits[0].agent_id.as_deref(), Some("agent-77"));
        assert!(commits[0].linked_note_id.is_none());
    }

    #[test]
    fn all_changed_paths_includes_untracked_and_modified() {
        let dir = init_repo("commit-changed");
        commit_file(dir.path(), "tracked.txt", "one\n");
        write_file(dir.path(), "tracked.txt", "two\n");
        write_file(dir.path(), "untracked.txt", "new\n");
        let mut paths = all_changed_paths(dir.path()).unwrap();
        paths.sort();
        assert_eq!(
            paths,
            vec!["tracked.txt".to_string(), "untracked.txt".to_string()]
        );
    }

    #[test]
    fn staged_paths_excludes_unstaged_and_untracked() {
        let dir = init_repo("commit-staged");
        commit_file(dir.path(), "tracked.txt", "one\n");
        write_file(dir.path(), "staged.txt", "s\n");
        stage(dir.path(), &["staged.txt".to_string()]).unwrap();
        write_file(dir.path(), "tracked.txt", "two\n");
        write_file(dir.path(), "untracked.txt", "new\n");
        assert_eq!(staged_paths(dir.path()).unwrap(), vec!["staged.txt"]);
    }

    #[test]
    fn staged_paths_is_empty_for_clean_index() {
        let dir = init_repo("commit-staged-clean");
        commit_file(dir.path(), "tracked.txt", "one\n");
        write_file(dir.path(), "tracked.txt", "two\n");
        assert!(staged_paths(dir.path()).unwrap().is_empty());
    }
}
