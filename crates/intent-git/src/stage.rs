//! Staging (`git.stage`).
//!
//! Ports `gitService.stageFiles` (`git add -- <paths>`): each path is normalized
//! relative to the worktree and staged. A path that is gone from the worktree
//! but tracked stages its deletion; a path that matches nothing errors like
//! `git add`. The CSV/array parse and the `.`/`*`/`--all` rejection are wire
//! policy and live in `intent-services` (the TS `ws.git.stage` builder).

use std::path::Path;

use git2::{ObjectType, Repository};
use intent_core::{Error, Result};

use crate::map_git_err;

/// Stage `paths` (already split and validated) in the worktree.
pub fn stage(worktree_path: &Path, paths: &[String]) -> Result<()> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let workdir = repo
        .workdir()
        .ok_or_else(|| Error::Internal("Repository has no working directory".to_string()))?
        .to_path_buf();
    let mut index = repo.index().map_err(map_git_err)?;
    for raw in paths {
        let rel = normalize_rel(&workdir, raw);
        let rel_path = Path::new(&rel);
        if workdir.join(&rel).exists() {
            index.add_path(rel_path).map_err(map_git_err)?;
        } else if index.get_path(rel_path, 0).is_some() {
            // Tracked but deleted from the worktree → stage the removal.
            index.remove_path(rel_path).map_err(map_git_err)?;
        } else {
            return Err(Error::Internal(format!(
                "fatal: pathspec '{raw}' did not match any files"
            )));
        }
    }
    index.write().map_err(map_git_err)?;
    Ok(())
}

/// Unstage `paths` (already split and validated), mirroring `git reset -- <paths>`:
/// each path's index entry is reset to its `HEAD` version, or removed from the
/// index when the path is absent from `HEAD` (a newly added file). With no
/// commit yet (unborn `HEAD`) every path is reset against the empty tree, so a
/// staged add is dropped from the index.
pub fn unstage(worktree_path: &Path, paths: &[String]) -> Result<()> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let workdir = repo
        .workdir()
        .ok_or_else(|| Error::Internal("Repository has no working directory".to_string()))?
        .to_path_buf();
    // The HEAD commit object backs `reset_default`; `None` (unborn HEAD) resets
    // the listed paths against the empty tree, dropping staged adds.
    let head = repo
        .head()
        .ok()
        .and_then(|h| h.target())
        .and_then(|oid| repo.find_object(oid, Some(ObjectType::Commit)).ok());
    let specs: Vec<String> = paths
        .iter()
        .map(|raw| normalize_rel(&workdir, raw))
        .collect();
    repo.reset_default(head.as_ref(), specs.iter())
        .map_err(map_git_err)?;
    Ok(())
}

/// Discard working-tree changes to `paths` (already split and validated),
/// mirroring `gitService.discardChanges`: tracked paths are restored from the
/// index (equivalent to `git checkout -- <paths>`), untracked paths are deleted
/// from disk (files unlinked, directories removed recursively). Idempotent on
/// clean files: a checkout against an unchanged path is a no-op, and a missing
/// untracked path (already deleted) is ignored (`ENOENT` race). Staged changes
/// are untouched — this discards only the unstaged worktree delta, matching
/// the reference. A pathspec naming a directory whose contents are tracked is
/// treated as tracked and routed through `checkout_index` (reference parity —
/// libgit2's exact-file index probe would otherwise miss it and `remove_dir_all`
/// would wipe tracked content). Untracked deletion is refused (surfaced as
/// `-32602 InvalidParams`) for any path that escapes the worktree (absolute
/// paths outside `workdir`, `..` traversal, or the empty / `.` root). A
/// non-ENOENT filesystem error on untracked deletion is best-effort (silently
/// swallowed, matching the reference's per-file warn-and-continue behavior);
/// tracked-batch checkout failures propagate as `Error::Git`.
pub fn discard(worktree_path: &Path, paths: &[String]) -> Result<()> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let workdir = repo
        .workdir()
        .ok_or_else(|| Error::Internal("Repository has no working directory".to_string()))?
        .to_path_buf();
    let index = repo.index().map_err(map_git_err)?;

    // Partition into tracked vs untracked, mirroring the reference's
    // `git ls-files --error-unmatch <path>` probe: a path is tracked iff it
    // has a stage-0 entry in the current index. Directories that *contain*
    // tracked files (index entries under a `<rel>/` prefix) are also treated
    // as tracked so `remove_dir_all` never wipes tracked content — the
    // reference guards this with `git ls-files --error-unmatch` on the
    // literal path but never issues `rm -rf` on a directory.
    // Validate + classify. `normalize_rel` returns a repo-relative path when
    // the input is inside the worktree; anything else (absolute path outside,
    // `.` / empty root, `..` traversal) is refused up-front as -32602 so
    // libgit2's index probe never sees a path it would panic on and
    // `remove_dir_all` can never target the worktree root or outside.
    let mut tracked: Vec<String> = Vec::new();
    let mut untracked: Vec<String> = Vec::new();
    for raw in paths {
        let rel = normalize_rel(&workdir, raw);
        if !is_safe_rel(&workdir, &rel) {
            return Err(Error::InvalidParams(format!(
                "Path escapes the worktree: {rel}"
            )));
        }
        if index.get_path(Path::new(&rel), 0).is_some() || index_has_dir_prefix(&index, &rel) {
            // Exact-file index entry OR a directory pathspec naming a tracked
            // subtree — go through `checkout_index`, matching the reference
            // where `git ls-files --error-unmatch <dir>` succeeds recursively
            // and the path is then checked out as a directory pathspec. This
            // is what prevents `remove_dir_all` from wiping tracked content
            // reachable via a directory pathspec (libgit2's exact-file index
            // probe would otherwise miss it).
            tracked.push(rel);
        } else {
            untracked.push(rel);
        }
    }

    // Tracked: restore worktree from index (`git checkout -- <paths>`).
    if !tracked.is_empty() {
        let mut opts = git2::build::CheckoutBuilder::new();
        // Overwrite modified working-tree files, do not touch the index.
        opts.force().update_index(false).remove_untracked(false);
        for rel in &tracked {
            opts.path(rel);
        }
        // `checkout_index` with `None` uses the repository's current index.
        repo.checkout_index(None, Some(&mut opts))
            .map_err(map_git_err)?;
    }

    // Untracked: delete from disk (files unlinked, directories recursively
    // removed). `ENOENT` is ignored — matches the reference's race-tolerant
    // deletion.
    for rel in &untracked {
        let full = workdir.join(rel);
        let meta = match std::fs::symlink_metadata(&full) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => continue,
        };
        if meta.file_type().is_dir() {
            let _ = std::fs::remove_dir_all(&full);
        } else {
            let _ = std::fs::remove_file(&full);
        }
    }

    Ok(())
}

/// True when `rel` is a non-empty, non-`.` relative path whose lexical
/// resolution against `workdir` stays strictly under `workdir`. Used by
/// `discard` to reject absolute paths outside the repo, `..` traversal, and
/// the worktree-root pathspec before touching either the index or the
/// filesystem.
fn is_safe_rel(workdir: &Path, rel: &str) -> bool {
    if rel.is_empty() || rel == "." {
        return false;
    }
    let candidate = Path::new(rel);
    if candidate.is_absolute() {
        return false;
    }
    let full = workdir.join(candidate);
    let normalized = lexical_normalize(&full);
    normalized.starts_with(workdir) && normalized != workdir
}

/// Returns true when the index contains any entry under the `<rel>/`
/// directory prefix — i.e. the pathspec names a tracked subtree, even
/// though the directory itself is not a stage-0 index entry.
fn index_has_dir_prefix(index: &git2::Index, rel: &str) -> bool {
    if rel.is_empty() {
        return false;
    }
    let mut prefix = rel.trim_end_matches('/').to_string();
    prefix.push('/');
    index
        .iter()
        .any(|entry| String::from_utf8_lossy(&entry.path).starts_with(&prefix))
}

/// Purely lexical path normalization: resolves `.` / `..` components against
/// `path` without touching the filesystem (unlike `canonicalize`, which
/// requires the path to exist and follows symlinks). Used by `discard` to
/// verify an untracked deletion target stays inside the worktree even when
/// the path is missing (idempotent-ENOENT branch).
fn lexical_normalize(path: &Path) -> std::path::PathBuf {
    let mut out = std::path::PathBuf::new();
    for comp in path.components() {
        match comp {
            std::path::Component::ParentDir => {
                out.pop();
            }
            std::path::Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Normalize a path to be relative to the worktree, mirroring the TS
/// `path.isAbsolute(p) ? path.relative(worktree, p) : p`.
fn normalize_rel(workdir: &Path, raw: &str) -> String {
    let p = Path::new(raw);
    if p.is_absolute() {
        p.strip_prefix(workdir)
            .map(|r| r.to_string_lossy().to_string())
            .unwrap_or_else(|_| raw.to_string())
    } else {
        raw.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::status;
    use crate::testutil::{commit_file, init_repo, write_file};
    use intent_core::GitFileStatus;

    #[test]
    fn stages_a_new_file() {
        let dir = init_repo("stage-new");
        commit_file(dir.path(), "seed.txt", "seed\n");
        write_file(dir.path(), "new.txt", "hi\n");
        stage(dir.path(), &["new.txt".to_string()]).unwrap();
        let st = status(dir.path()).unwrap();
        let f = st.files.iter().find(|f| f.path == "new.txt").unwrap();
        assert!(f.staged);
        assert_eq!(f.status, GitFileStatus::Added);
    }

    #[test]
    fn stages_a_deletion() {
        let dir = init_repo("stage-del");
        commit_file(dir.path(), "gone.txt", "bye\n");
        std::fs::remove_file(dir.path().join("gone.txt")).unwrap();
        stage(dir.path(), &["gone.txt".to_string()]).unwrap();
        let st = status(dir.path()).unwrap();
        let f = st.files.iter().find(|f| f.path == "gone.txt").unwrap();
        assert!(f.staged);
        assert_eq!(f.status, GitFileStatus::Deleted);
    }

    #[test]
    fn unstage_reverts_a_staged_modification() {
        let dir = init_repo("unstage-mod");
        commit_file(dir.path(), "a.txt", "one\n");
        write_file(dir.path(), "a.txt", "two\n");
        stage(dir.path(), &["a.txt".to_string()]).unwrap();
        unstage(dir.path(), &["a.txt".to_string()]).unwrap();
        let st = status(dir.path()).unwrap();
        let f = st.files.iter().find(|f| f.path == "a.txt").unwrap();
        assert!(!f.staged);
        assert_eq!(f.status, GitFileStatus::Modified);
    }

    #[test]
    fn unstage_drops_a_staged_add_from_the_index() {
        let dir = init_repo("unstage-add");
        commit_file(dir.path(), "seed.txt", "seed\n");
        write_file(dir.path(), "new.txt", "hi\n");
        stage(dir.path(), &["new.txt".to_string()]).unwrap();
        unstage(dir.path(), &["new.txt".to_string()]).unwrap();
        let st = status(dir.path()).unwrap();
        let f = st.files.iter().find(|f| f.path == "new.txt").unwrap();
        assert!(!f.staged);
        assert_eq!(f.status, GitFileStatus::Untracked);
    }

    #[test]
    fn unmatched_pathspec_errors() {
        let dir = init_repo("stage-miss");
        commit_file(dir.path(), "seed.txt", "seed\n");
        let err = stage(dir.path(), &["nope.txt".to_string()]).unwrap_err();
        assert!(format!("{err}").contains("did not match any files"));
    }

    #[test]
    fn discard_reverts_an_unstaged_modification() {
        let dir = init_repo("discard-mod");
        commit_file(dir.path(), "a.txt", "one\n");
        write_file(dir.path(), "a.txt", "two\n");
        discard(dir.path(), &["a.txt".to_string()]).unwrap();
        let on_disk = std::fs::read_to_string(dir.path().join("a.txt")).unwrap();
        assert_eq!(on_disk, "one\n");
        let st = status(dir.path()).unwrap();
        assert!(st.files.iter().all(|f| f.path != "a.txt"));
    }

    #[test]
    fn discard_deletes_untracked_files() {
        let dir = init_repo("discard-untracked");
        commit_file(dir.path(), "seed.txt", "seed\n");
        write_file(dir.path(), "new.txt", "hi\n");
        assert!(dir.path().join("new.txt").exists());
        discard(dir.path(), &["new.txt".to_string()]).unwrap();
        assert!(!dir.path().join("new.txt").exists());
        let st = status(dir.path()).unwrap();
        assert!(st.files.iter().all(|f| f.path != "new.txt"));
    }

    #[test]
    fn discard_deletes_untracked_directory_recursively() {
        let dir = init_repo("discard-untracked-dir");
        commit_file(dir.path(), "seed.txt", "seed\n");
        let sub = dir.path().join("newdir");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(sub.join("a.txt"), "a\n").unwrap();
        std::fs::write(sub.join("b.txt"), "b\n").unwrap();
        discard(dir.path(), &["newdir".to_string()]).unwrap();
        assert!(!sub.exists());
    }

    #[test]
    fn discard_preserves_staged_changes() {
        let dir = init_repo("discard-staged");
        commit_file(dir.path(), "a.txt", "one\n");
        // Stage a modification, then dirty the worktree further.
        write_file(dir.path(), "a.txt", "two\n");
        stage(dir.path(), &["a.txt".to_string()]).unwrap();
        write_file(dir.path(), "a.txt", "three\n");
        // Discard drops the unstaged "three" back to the staged "two"; the
        // staged entry itself is preserved (matches `git checkout -- a.txt`).
        discard(dir.path(), &["a.txt".to_string()]).unwrap();
        let on_disk = std::fs::read_to_string(dir.path().join("a.txt")).unwrap();
        assert_eq!(on_disk, "two\n");
        let st = status(dir.path()).unwrap();
        let f = st.files.iter().find(|f| f.path == "a.txt").unwrap();
        assert!(f.staged);
        assert_eq!(f.status, GitFileStatus::Modified);
    }

    #[test]
    fn discard_is_idempotent_on_clean_files() {
        let dir = init_repo("discard-clean");
        commit_file(dir.path(), "a.txt", "one\n");
        discard(dir.path(), &["a.txt".to_string()]).unwrap();
        // A second call on the same clean path is still ok.
        discard(dir.path(), &["a.txt".to_string()]).unwrap();
        let on_disk = std::fs::read_to_string(dir.path().join("a.txt")).unwrap();
        assert_eq!(on_disk, "one\n");
    }

    #[test]
    fn discard_missing_untracked_path_is_ignored() {
        // ENOENT parity with the reference's race-tolerant unlink: an untracked
        // path that no longer exists on disk does not error.
        let dir = init_repo("discard-missing");
        commit_file(dir.path(), "seed.txt", "seed\n");
        discard(dir.path(), &["nope.txt".to_string()]).unwrap();
    }

    #[test]
    fn discard_directory_of_tracked_files_restores_from_index() {
        // A pathspec naming a directory whose contents are tracked in the
        // index must go through `checkout_index` (not `rm -rf`). Reference
        // parity: `ls-files --error-unmatch <dir>` succeeds recursively, so
        // the FE reaches the `git checkout -- <dir>` branch and dirty files
        // under it are restored. libgit2's index probe is exact-file, so we
        // extend the tracked classification with a directory-prefix check.
        let dir = init_repo("discard-tracked-dir");
        commit_file(dir.path(), "src/a.txt", "a\n");
        commit_file(dir.path(), "src/b.txt", "b\n");
        // Dirty the tracked files under the directory.
        write_file(dir.path(), "src/a.txt", "dirty a\n");
        write_file(dir.path(), "src/b.txt", "dirty b\n");
        discard(dir.path(), &["src".to_string()]).unwrap();
        // Restored to the index (HEAD) content, not deleted.
        assert!(dir.path().join("src/a.txt").exists());
        assert!(dir.path().join("src/b.txt").exists());
        assert_eq!(
            std::fs::read_to_string(dir.path().join("src/a.txt")).unwrap(),
            "a\n"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("src/b.txt")).unwrap(),
            "b\n"
        );
    }

    #[test]
    fn discard_refuses_traversal_outside_worktree() {
        // A relative pathspec containing `..` that resolves outside the
        // worktree must be refused rather than deleted.
        let dir = init_repo("discard-traversal");
        commit_file(dir.path(), "seed.txt", "seed\n");
        // `../evil.txt` is untracked (no index entry, no prefix match),
        // resolves to a sibling of the worktree — must fail.
        let err = discard(dir.path(), &["../evil.txt".to_string()]).unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)));
    }

    #[test]
    fn discard_refuses_absolute_path_outside_worktree() {
        // An absolute path outside the worktree survives `normalize_rel`
        // unchanged (its `strip_prefix` fails) and must be refused rather
        // than deleted.
        let dir = init_repo("discard-abs");
        commit_file(dir.path(), "seed.txt", "seed\n");
        let err = discard(dir.path(), &["/tmp/nope-outside-worktree".to_string()]).unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)));
    }

    #[test]
    fn discard_refuses_dot_and_empty() {
        // Router-level validation already rejects `.` / `*` / `--all`; this
        // is a defense-in-depth guard against a caller sneaking `.` in via
        // the array shape and having the deletion loop target the whole
        // worktree via `remove_dir_all`.
        let dir = init_repo("discard-dot");
        commit_file(dir.path(), "seed.txt", "seed\n");
        let err = discard(dir.path(), &[".".to_string()]).unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)));
        let err = discard(dir.path(), &["".to_string()]).unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)));
    }
}
