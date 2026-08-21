//! File-at-revision reads (`git.showFile`).
//!
//! Ports the legacy `git:show-file` IPC (`git show <ref>:<path>`): the raw file
//! content at a revision, backing the FE diff viewers / PR section / commits
//! timeline. `ref` accepts anything revparse-able (commit hash, branch, `HEAD`,
//! `<hash>^`, …) plus the index ref `":0"` (a stage-0 index entry — the TS diff
//! viewer compares `HEAD` against `":0"` for staged hunks). A path missing at
//! the ref (e.g. a new file) yields an empty string, mirroring the legacy
//! handler's "does not exist → ''" fallback; an unresolvable ref is an error.

use std::path::Path;

use git2::Repository;
use intent_core::{Error, Result};

use crate::map_git_err;

/// Gitlink (submodule pin) tree-entry mode.
const MODE_GITLINK: u32 = 0o160_000;

/// Read `file_path` at `refname` from the repository at `worktree_path`.
///
/// `file_path` may be worktree-relative or absolute; an absolute path under
/// `worktree_path` is made relative (the legacy handler's boundary-checked
/// conversion). Returns `Ok("")` when the path does not exist at the ref.
/// A path that resolves to a non-blob entry — a `160000` gitlink (submodule
/// pin) or a `040000` tree — yields the typed [`Error::NotAFile`] instead of
/// content (monorepo#1739): a gitlink's id is a commit in the **submodule's**
/// odb, so there is no blob to read here.
pub fn show_file(worktree_path: &Path, refname: &str, file_path: &str) -> Result<String> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    let rel = relative_path(worktree_path, file_path);
    if rel.is_empty() {
        return Ok(String::new());
    }

    if let Some(stage) = index_stage(refname) {
        let index = repo.index().map_err(map_git_err)?;
        let Some(entry) = index.get_path(Path::new(&rel), stage) else {
            return Ok(String::new());
        };
        if entry.mode == MODE_GITLINK {
            return Err(not_a_file(&rel, entry.mode));
        }
        let blob = repo.find_blob(entry.id).map_err(map_git_err)?;
        return Ok(String::from_utf8_lossy(blob.content()).into_owned());
    }

    let object = repo.revparse_single(refname).map_err(map_git_err)?;
    let tree = object.peel_to_tree().map_err(map_git_err)?;
    let entry = match tree.get_path(Path::new(&rel)) {
        Ok(e) => e,
        // Missing path at the ref (new file) → empty content, not an error.
        Err(e) if e.code() == git2::ErrorCode::NotFound => return Ok(String::new()),
        Err(e) => return Err(map_git_err(e)),
    };
    // A gitlink's to_object() would fail (the pin commit is not in this odb),
    // so route on the tree-entry mode before dereferencing.
    let mode = entry.filemode() as u32;
    if mode == MODE_GITLINK {
        return Err(not_a_file(&rel, mode));
    }
    let object = entry.to_object(&repo).map_err(map_git_err)?;
    match object.as_blob() {
        Some(blob) => Ok(String::from_utf8_lossy(blob.content()).into_owned()),
        // The path resolves to a tree, not file content.
        None => Err(not_a_file(&rel, mode)),
    }
}

/// Build the typed non-blob error with the octal mode string (e.g. `"160000"`).
fn not_a_file(path: &str, mode: u32) -> Error {
    Error::NotAFile {
        path: path.to_string(),
        mode: format!("{mode:06o}"),
    }
}

/// Parse an index ref (`":0"`, `":1"`, …) into its stage number; `None` for
/// ordinary revparse-able refs. A bare `":"` is treated as stage 0, matching
/// `git show :<path>`.
fn index_stage(refname: &str) -> Option<i32> {
    let rest = refname.strip_prefix(':')?;
    if rest.is_empty() {
        return Some(0);
    }
    rest.parse::<i32>().ok()
}

/// The legacy handler's absolute→relative conversion with a directory-boundary
/// check: an absolute `file_path` equal to the worktree becomes empty; one
/// under `<worktree>/` is stripped; anything else passes through verbatim.
fn relative_path(worktree_path: &Path, file_path: &str) -> String {
    if !file_path.starts_with('/') {
        return file_path.to_string();
    }
    let worktree = worktree_path.to_string_lossy();
    if file_path == worktree {
        return String::new();
    }
    match file_path.strip_prefix(&format!("{worktree}/")) {
        Some(rel) => rel.to_string(),
        None => file_path.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{commit_file, init_repo, write_file};

    #[test]
    fn reads_content_at_head_and_earlier_refs() {
        let tmp = init_repo("show");
        commit_file(tmp.path(), "a.txt", "v1");
        commit_file(tmp.path(), "a.txt", "v2");
        assert_eq!(show_file(tmp.path(), "HEAD", "a.txt").unwrap(), "v2");
        assert_eq!(show_file(tmp.path(), "HEAD^", "a.txt").unwrap(), "v1");
    }

    #[test]
    fn absolute_path_under_worktree_is_made_relative() {
        let tmp = init_repo("show-abs");
        commit_file(tmp.path(), "dir/b.txt", "hello");
        let abs = tmp.path().join("dir/b.txt");
        assert_eq!(
            show_file(tmp.path(), "HEAD", &abs.to_string_lossy()).unwrap(),
            "hello"
        );
    }

    #[test]
    fn missing_path_at_ref_is_empty_not_error() {
        let tmp = init_repo("show-missing");
        commit_file(tmp.path(), "a.txt", "v1");
        assert_eq!(show_file(tmp.path(), "HEAD", "nope.txt").unwrap(), "");
    }

    #[test]
    fn index_ref_reads_staged_content() {
        let tmp = init_repo("show-index");
        commit_file(tmp.path(), "a.txt", "committed");
        write_file(tmp.path(), "a.txt", "staged");
        crate::stage::stage(tmp.path(), &["a.txt".to_string()]).unwrap();
        assert_eq!(show_file(tmp.path(), ":0", "a.txt").unwrap(), "staged");
        // Unstaged path at the index ref → empty.
        assert_eq!(show_file(tmp.path(), ":0", "nope.txt").unwrap(), "");
    }

    #[test]
    fn bad_ref_is_an_error() {
        let tmp = init_repo("show-badref");
        commit_file(tmp.path(), "a.txt", "v1");
        assert!(show_file(tmp.path(), "no-such-ref", "a.txt").is_err());
    }

    /// A gitlink path yields the typed `NotAFile` error — with the octal mode —
    /// at both a tree ref and the index ref (monorepo#1739).
    #[test]
    fn gitlink_path_is_typed_not_a_file_error() {
        let tmp = init_repo("show-gitlink");
        commit_file(tmp.path(), "a.txt", "v1");
        crate::testutil::commit_gitlink_bump(
            tmp.path(),
            "sub",
            "7257a190564088376227525989c4994e46082ad1",
        );
        for refname in ["HEAD", ":0"] {
            let err = show_file(tmp.path(), refname, "sub").unwrap_err();
            match err {
                intent_core::Error::NotAFile { path, mode } => {
                    assert_eq!(path, "sub");
                    assert_eq!(mode, "160000");
                }
                other => panic!("expected NotAFile at {refname}, got {other:?}"),
            }
        }
        // Regular files at both refs still read fine.
        assert_eq!(show_file(tmp.path(), "HEAD", "a.txt").unwrap(), "v1");
    }

    /// A directory (tree entry) also yields `NotAFile`, with the tree mode.
    #[test]
    fn tree_path_is_typed_not_a_file_error() {
        let tmp = init_repo("show-tree");
        commit_file(tmp.path(), "dir/b.txt", "hello");
        let err = show_file(tmp.path(), "HEAD", "dir").unwrap_err();
        match err {
            intent_core::Error::NotAFile { path, mode } => {
                assert_eq!(path, "dir");
                assert_eq!(mode, "040000");
            }
            other => panic!("expected NotAFile, got {other:?}"),
        }
    }
}
