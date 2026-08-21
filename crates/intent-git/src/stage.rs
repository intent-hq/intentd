//! Staging (`git.stage`).
//!
//! Ports `gitService.stageFiles` (`git add -- <paths>`): each path is normalized
//! relative to the worktree and staged. A path that is gone from the worktree
//! but tracked stages its deletion; a path that matches nothing errors like
//! `git add`. The CSV/array parse and the `.`/`*`/`--all` rejection are wire
//! policy and live in `intent-services` (the TS `ws.git.stage` builder).

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use git2::{ObjectType, Repository};
use intent_core::{Error, Result};

use crate::map_git_err;
use crate::submodule::{
    ignores_case, is_submodule_path, reject_submodule_internal_paths, submodule_paths,
};

/// Stage `paths` (already split and validated) in the worktree. Refuses any
/// path strictly inside a registered submodule (parity with `git add`'s
/// "is in submodule" pathspec error) before touching the index, so a caller
/// can never flatten a submodule's gitlink into the superproject
/// (monorepo#1714). Staging the gitlink path itself (a pin bump) is unaffected.
/// The guard matches on the repo-relative form of each path, so an in-worktree
/// absolute path (`/repo/sub/a.txt`) is refused exactly like its relative
/// spelling — it would otherwise slip past the guard and be normalized into
/// `sub/a.txt` on the way to `index.add_path`.
pub fn stage(worktree_path: &Path, paths: &[String]) -> Result<()> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    reject_submodule_internal_paths(&repo, paths)?;
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
///
/// A path strictly inside a registered submodule is refused with the same
/// "is in submodule" error as [`stage`] (monorepo#1733): the superproject index
/// has no entry for it, so it would otherwise classify as untracked and be
/// unlinked, destroying an uncommitted submodule edit — real
/// `git checkout -- sub/a.txt` refuses. The guard runs before the
/// tracked/untracked partition and before any filesystem mutation, so a batch
/// containing one such path discards nothing at all. The gitlink path itself
/// stays allowed *while it is still a stage-0 `160000` index entry*: checking
/// out that entry is a benign no-op. A registered submodule with no such entry
/// (staged for removal via `git rm --cached`) is refused instead — the
/// partition would otherwise find nothing in the index, classify it as
/// untracked and `remove_dir_all` the whole submodule checkout, where real
/// `git checkout -- sub` errors and touches nothing.
///
/// An *ancestor* directory pathspec (`packages` when the submodule is
/// `packages/intentd`) is deliberately not refused — the submodule is inside
/// it, not the other way round. It stays safe because the index entries under
/// the `packages/` prefix classify it as tracked, routing it through
/// `checkout_index`, which leaves the `160000` entry alone.
pub fn discard(worktree_path: &Path, paths: &[String]) -> Result<()> {
    let repo = Repository::open(worktree_path).map_err(map_git_err)?;
    reject_submodule_internal_paths(&repo, paths)?;
    let submodules = submodule_paths(&repo)?;
    let ignore_case = ignores_case(&repo);
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
        if index.get_path(Path::new(&rel), 0).is_none()
            && is_submodule_path(&submodules, &rel, ignore_case)
        {
            // A registered submodule whose gitlink is no longer a stage-0
            // index entry (`git rm --cached sub`). The allow above assumes the
            // `160000` checkout no-op, which does not exist here; without this
            // refusal the path falls into the untracked bucket and
            // `remove_dir_all` takes the submodule checkout with it. Real
            // `git checkout -- sub` errors on the unmatched pathspec.
            return Err(Error::Internal(format!(
                "fatal: pathspec '{raw}' did not match any files"
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

/// True when `rel` is a non-empty, non-`.` relative path with no `..`
/// components whose lexical resolution against `workdir` stays strictly
/// under `workdir`. Used by `discard` to reject absolute paths outside the
/// repo, any `..` traversal (including bypasses like `a/../tracked.txt`
/// whose lexical target is inside the worktree but which the OS would
/// resolve at deletion time to unlink a tracked file), and the
/// worktree-root pathspec before touching either the index or the
/// filesystem.
fn is_safe_rel(workdir: &Path, rel: &str) -> bool {
    if rel.is_empty() || rel == "." {
        return false;
    }
    let candidate = Path::new(rel);
    if candidate.is_absolute() {
        return false;
    }
    // Reject ANY `..` component outright — even if it lexically normalizes
    // to a path inside `workdir`, the classification step matches the index
    // against the un-normalized string (so `a/../tracked.txt` misses the
    // exact-file probe and falls through to untracked deletion, where the
    // OS resolves `..` and unlinks the tracked file). Refusing `..` up-front
    // closes that bypass.
    for comp in candidate.components() {
        if matches!(comp, std::path::Component::ParentDir) {
            return false;
        }
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
/// `path.isAbsolute(p) ? path.relative(worktree, p) : p`, then stripping
/// lexical `.` noise via [`drop_curdir`].
fn normalize_rel(workdir: &Path, raw: &str) -> String {
    let p = Path::new(raw);
    if p.is_absolute() {
        p.strip_prefix(workdir)
            .map_or_else(|_| drop_curdir(p), drop_curdir)
    } else {
        drop_curdir(p)
    }
}

/// Drop `CurDir` (`.`) components from `p`, the spelling noise that
/// `Path::components` preserves at the head of a relative path (`./sub`).
/// Without this the classification below probes the index with a literal
/// `./sub`, which libgit2 rejects outright ("repo path should not start with
/// `.`"), and the path falls through to untracked deletion (monorepo#1733).
/// `ParentDir` is deliberately preserved so [`is_safe_rel`]'s `..` refusal
/// still sees it.
fn drop_curdir(p: &Path) -> String {
    p.components()
        .filter(|c| !matches!(c, std::path::Component::CurDir))
        .map(std::path::Component::as_os_str)
        .collect::<std::path::PathBuf>()
        .to_string_lossy()
        .to_string()
}

/// Apply a unified-diff `patch` to the index only (`git apply --cached`),
/// staging one or more hunks without staging the rest of `file_path`. Mirrors
/// `gitService.stageHunk`: a direct apply is attempted first, then a `--3way`
/// retry to tolerate small context mismatches. Shells out to `git` because
/// libgit2's `apply` API does not implement the three-way fallback. `file_path`
/// is validated against every `diff --git a/… b/…` header in `patch` so a
/// client cannot slip a multi-file or path-mismatched patch through the
/// single-file wire contract (`-32602` on mismatch). The patch is streamed on
/// stdin so no temp file is written.
pub fn stage_hunk(worktree_path: &Path, file_path: &str, patch: &str) -> Result<()> {
    validate_single_file_patch(file_path, patch)?;
    apply_patch_cached(worktree_path, patch, false)
}

/// Reverse-apply a unified-diff `patch` to the index only
/// (`git apply --cached --reverse`), unstaging one or more hunks without
/// unstaging the rest of `file_path`. Mirrors `gitService.unstageHunk`; the
/// direct-then-`--3way` fallback and the single-file header check match
/// [`stage_hunk`].
pub fn unstage_hunk(worktree_path: &Path, file_path: &str, patch: &str) -> Result<()> {
    validate_single_file_patch(file_path, patch)?;
    apply_patch_cached(worktree_path, patch, true)
}

/// Reject patches that either target more than one file or reference a path
/// other than `file_path`. Enforces the single-file contract advertised by
/// `git.stageHunk`/`git.unstageHunk` — otherwise a client could pass an
/// arbitrary multi-file patch through `hunkPatch` and have the daemon apply
/// it, hiding the real touch set behind the `filePath` parameter. Returns
/// [`Error::InvalidParams`] so the router surfaces `-32602`.
fn validate_single_file_patch(file_path: &str, patch: &str) -> Result<()> {
    let expected = file_path.trim();
    if expected.is_empty() {
        return Err(Error::InvalidParams("filePath is required".to_string()));
    }
    // git emits `diff --git a/PATH b/PATH` without quoting for paths that
    // contain spaces (only non-ASCII / control chars trigger c-style quoting
    // by default), so a path whose own name contains ` b/` (e.g.
    // `dir b/file.txt`) makes the header ambiguous to split on ` b/` — the
    // first, last, or any middle occurrence could be the a/b separator. The
    // header shape is fully determined by `expected`, so compare against the
    // exact synthesized header instead of trying to split.
    let expected_header = format!("a/{expected} b/{expected}");
    let mut header_count = 0usize;
    for raw in patch.lines() {
        let Some(rest) = raw.strip_prefix("diff --git ") else {
            continue;
        };
        header_count += 1;
        if rest != expected_header {
            return Err(Error::InvalidParams(format!(
                "hunkPatch targets `{rest}`, expected `{expected_header}`"
            )));
        }
    }
    if header_count == 0 {
        return Err(Error::InvalidParams(
            "hunkPatch is missing a `diff --git a/… b/…` header".to_string(),
        ));
    }
    Ok(())
}

/// Run `git apply --cached [--reverse]` with `patch` on stdin, retrying with
/// `--3way` when the strict apply fails. The retry order mirrors the reference
/// FE (`gitService.stageHunk`/`unstageHunk`) so context-mismatch tolerance is
/// consistent across ports. When both attempts fail, the error surfaces the
/// stderr from the final (`--3way`) attempt with the direct-apply stderr
/// appended for context — the 3-way run is the one that decided the patch is
/// unapplyable, so its diagnostics are what an operator needs to act on.
fn apply_patch_cached(worktree_path: &Path, patch: &str, reverse: bool) -> Result<()> {
    let base_args: &[&str] = if reverse {
        &["apply", "--cached", "--reverse"]
    } else {
        &["apply", "--cached"]
    };
    match run_git_apply(worktree_path, base_args, patch) {
        Ok(()) => Ok(()),
        Err(direct_err) => {
            let three_way: Vec<&str> = base_args.iter().copied().chain(["--3way"]).collect();
            match run_git_apply(worktree_path, &three_way, patch) {
                Ok(()) => Ok(()),
                Err(three_way_err) => Err(Error::Internal(format!(
                    "{three_way_err} (direct-apply error: {direct_err})"
                ))),
            }
        }
    }
}

/// Spawn `git <args>` in `worktree_path` and pipe `patch` on stdin. Non-zero
/// exit surfaces as [`Error::Internal`] with the stderr the git binary produced.
fn run_git_apply(worktree_path: &Path, args: &[&str], patch: &str) -> Result<()> {
    let mut child = Command::new("git")
        .current_dir(worktree_path)
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| Error::Internal(format!("failed to spawn git: {e}")))?;
    {
        let stdin = child
            .stdin
            .as_mut()
            .ok_or_else(|| Error::Internal("git stdin unavailable".to_string()))?;
        stdin
            .write_all(patch.as_bytes())
            .map_err(|e| Error::Internal(format!("failed to write patch: {e}")))?;
    }
    let out = child
        .wait_with_output()
        .map_err(|e| Error::Internal(format!("failed to wait for git: {e}")))?;
    if out.status.success() {
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
        Err(Error::Internal(if stderr.is_empty() {
            format!("git {} failed", args.join(" "))
        } else {
            stderr
        }))
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
    fn stage_rejects_submodule_internal_path() {
        use crate::testutil::add_submodule;
        let child = init_repo("stage-sub-child");
        commit_file(child.path(), "a.txt", "a\n");
        let dir = init_repo("stage-sub-parent");
        commit_file(dir.path(), "seed.txt", "seed\n");
        add_submodule(dir.path(), child.path(), "sub");
        // Dirty a file *inside* the submodule's own worktree.
        write_file(&dir.path().join("sub"), "a.txt", "changed\n");
        let err = stage(dir.path(), &["sub/a.txt".to_string()]).unwrap_err();
        assert!(
            format!("{err}").contains("is in submodule"),
            "unexpected error: {err}"
        );
        // Nothing was staged: index still has the gitlink, not blobs.
        let st = status(dir.path()).unwrap();
        assert!(
            st.files.iter().all(|f| f.path != "sub/a.txt"),
            "submodule-internal path must not be staged: {st:?}"
        );
    }

    /// Regression: the guard must run on the *normalized* (repo-relative)
    /// path. An in-worktree absolute path does not match the registered
    /// relative submodule path (`sub`), so a guard that inspected the raw
    /// spelling let it through and `normalize_rel` then handed
    /// `sub/a.txt` to `index.add_path` — flattening the gitlink after all.
    #[test]
    fn stage_rejects_absolute_submodule_internal_path() {
        use crate::testutil::add_submodule;
        let child = init_repo("stage-sub-child-abs");
        commit_file(child.path(), "a.txt", "a\n");
        let dir = init_repo("stage-sub-parent-abs");
        commit_file(dir.path(), "seed.txt", "seed\n");
        add_submodule(dir.path(), child.path(), "sub");
        write_file(&dir.path().join("sub"), "a.txt", "changed\n");

        let abs = dir.path().join("sub").join("a.txt");
        let err = stage(dir.path(), &[abs.to_string_lossy().to_string()]).unwrap_err();
        assert!(
            format!("{err}").contains("is in submodule"),
            "unexpected error: {err}"
        );
        // The gitlink survives: nothing under `sub/` reached the index.
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
        let st = status(dir.path()).unwrap();
        assert!(
            st.files.iter().all(|f| !f.path.starts_with("sub/")),
            "submodule-internal path must not be staged: {st:?}"
        );
    }

    /// Regression (case-variant bypass): with `core.ignorecase` — git's default
    /// on macOS/Windows — `SUB/a.txt` resolves to the same file on disk as
    /// `sub/a.txt`, so a byte-exact guard missed it and `index.add_path` wrote a
    /// flattened `SUB` tree over the `160000` gitlink. Real `git add SUB/a.txt`
    /// is a no-op that keeps the gitlink. `core.ignorecase` is set explicitly so
    /// the comparison logic is exercised on case-sensitive filesystems too.
    #[test]
    fn stage_rejects_case_variant_submodule_internal_path() {
        use crate::testutil::add_submodule;
        let child = init_repo("stage-sub-child-case");
        commit_file(child.path(), "a.txt", "a\n");
        let dir = init_repo("stage-sub-parent-case");
        commit_file(dir.path(), "seed.txt", "seed\n");
        add_submodule(dir.path(), child.path(), "sub");
        Repository::open(dir.path())
            .unwrap()
            .config()
            .unwrap()
            .set_bool("core.ignorecase", true)
            .unwrap();
        write_file(&dir.path().join("sub"), "a.txt", "changed\n");

        let err = stage(dir.path(), &["SUB/a.txt".to_string()]).unwrap_err();
        assert!(
            format!("{err}").contains("is in submodule"),
            "unexpected error: {err}"
        );
        // The gitlink survives: nothing under `sub/`/`SUB/` reached the index.
        let repo = Repository::open(dir.path()).unwrap();
        let index = repo.index().unwrap();
        assert!(
            index.iter().all(|e| {
                let p = String::from_utf8_lossy(&e.path).to_lowercase();
                !p.starts_with("sub/")
            }),
            "no submodule-internal blob may be in the index"
        );
        assert_eq!(
            index.get_path(Path::new("sub"), 0).unwrap().mode,
            u32::from(git2::FileMode::Commit),
            "gitlink entry intact"
        );
    }

    /// The case-variant guard must not swallow a *sibling* directory that
    /// merely shares a prefix with the submodule path.
    #[test]
    fn stage_allows_sibling_prefix_directory_under_ignorecase() {
        use crate::testutil::add_submodule;
        let child = init_repo("stage-sub-child-sibling");
        commit_file(child.path(), "a.txt", "a\n");
        let dir = init_repo("stage-sub-parent-sibling");
        commit_file(dir.path(), "seed.txt", "seed\n");
        add_submodule(dir.path(), child.path(), "sub");
        Repository::open(dir.path())
            .unwrap()
            .config()
            .unwrap()
            .set_bool("core.ignorecase", true)
            .unwrap();
        write_file(dir.path(), "subdir/a.txt", "sibling\n");

        stage(dir.path(), &["subdir/a.txt".to_string()]).unwrap();
        let st = status(dir.path()).unwrap();
        let f = st.files.iter().find(|f| f.path == "subdir/a.txt");
        assert!(
            f.is_some() && f.unwrap().staged,
            "sibling path staged: {st:?}"
        );
    }

    #[test]
    fn stage_gitlink_path_itself_succeeds() {
        use crate::testutil::add_submodule;
        let child = init_repo("stage-sub-child-pin");
        commit_file(child.path(), "a.txt", "a\n");
        let dir = init_repo("stage-sub-parent-pin");
        commit_file(dir.path(), "seed.txt", "seed\n");
        add_submodule(dir.path(), child.path(), "sub");
        // Bump the submodule's checked-out commit (simulating a pointer bump)
        // by re-pointing the submodule's worktree to a different commit via a
        // fresh commit inside the submodule.
        commit_file(&dir.path().join("sub"), "b.txt", "b\n");
        // Staging the gitlink path itself (not a path inside it) must succeed.
        stage(dir.path(), &["sub".to_string()]).unwrap();
        let st = status(dir.path()).unwrap();
        let f = st.files.iter().find(|f| f.path == "sub");
        assert!(f.is_some() && f.unwrap().staged, "gitlink pin bump staged");
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
    fn discard_refuses_traversal_bypass_into_worktree() {
        // Regression: a relative pathspec like `a/../tracked.txt` lexically
        // normalizes to a target inside the worktree, so a naive containment
        // check would let it through — but the untracked-branch deletion
        // uses `workdir.join(rel)` which the OS resolves at unlink time and
        // would unlink the tracked file. Refuse any `..` component up-front.
        let dir = init_repo("discard-traversal-bypass");
        commit_file(dir.path(), "tracked.txt", "tracked\n");
        let err = discard(dir.path(), &["a/../tracked.txt".to_string()]).unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)));
        // The tracked file must still be intact.
        assert_eq!(
            std::fs::read_to_string(dir.path().join("tracked.txt")).unwrap(),
            "tracked\n"
        );
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
        let err = discard(dir.path(), &[String::new()]).unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)));
    }

    /// Build a superproject with a registered submodule at `sub` whose own
    /// worktree carries an uncommitted edit to `a.txt`, the fixture shape for
    /// the discard submodule-guard regressions below.
    fn submodule_fixture(name: &str) -> (crate::testutil::TempDir, crate::testutil::TempDir) {
        use crate::testutil::add_submodule;
        let child = init_repo(&format!("{name}-child"));
        commit_file(child.path(), "a.txt", "a\n");
        let dir = init_repo(&format!("{name}-parent"));
        commit_file(dir.path(), "seed.txt", "seed\n");
        add_submodule(dir.path(), child.path(), "sub");
        write_file(&dir.path().join("sub"), "a.txt", "uncommitted\n");
        (dir, child)
    }

    /// Assert the submodule's uncommitted working-copy edit survived and the
    /// superproject still records `sub` as a `160000` gitlink.
    fn assert_submodule_intact(worktree: &Path) {
        assert_eq!(
            std::fs::read_to_string(worktree.join("sub").join("a.txt")).unwrap(),
            "uncommitted\n",
            "submodule working-copy file must survive"
        );
        let repo = Repository::open(worktree).unwrap();
        let index = repo.index().unwrap();
        assert_eq!(
            index.get_path(Path::new("sub"), 0).unwrap().mode,
            u32::from(git2::FileMode::Commit),
            "gitlink entry intact"
        );
    }

    /// Regression (monorepo#1733): `discard` classified a submodule-internal
    /// path as untracked in the superproject and unlinked it, destroying the
    /// submodule's uncommitted edit. Real `git checkout -- sub/a.txt` refuses.
    #[test]
    fn discard_rejects_submodule_internal_path() {
        let (dir, _child) = submodule_fixture("discard-sub");
        let err = discard(dir.path(), &["sub/a.txt".to_string()]).unwrap_err();
        assert!(
            format!("{err}").contains("is in submodule"),
            "unexpected error: {err}"
        );
        assert_submodule_intact(dir.path());
    }

    /// The guard matches the repo-relative form, so an in-worktree absolute
    /// spelling is refused exactly like its relative one.
    #[test]
    fn discard_rejects_absolute_submodule_internal_path() {
        let (dir, _child) = submodule_fixture("discard-sub-abs");
        let abs = dir.path().join("sub").join("a.txt");
        let err = discard(dir.path(), &[abs.to_string_lossy().to_string()]).unwrap_err();
        assert!(
            format!("{err}").contains("is in submodule"),
            "unexpected error: {err}"
        );
        assert_submodule_intact(dir.path());
    }

    /// With `core.ignorecase` (git's default on macOS/Windows) `SUB/a.txt`
    /// names the same file on disk, so it must be refused too. Set explicitly
    /// so the comparison is exercised on case-sensitive filesystems as well.
    #[test]
    fn discard_rejects_case_variant_submodule_internal_path() {
        let (dir, _child) = submodule_fixture("discard-sub-case");
        Repository::open(dir.path())
            .unwrap()
            .config()
            .unwrap()
            .set_bool("core.ignorecase", true)
            .unwrap();
        let err = discard(dir.path(), &["SUB/a.txt".to_string()]).unwrap_err();
        assert!(
            format!("{err}").contains("is in submodule"),
            "unexpected error: {err}"
        );
        assert_submodule_intact(dir.path());
    }

    /// The batch is atomic: one submodule-internal path refuses the whole
    /// call, and the innocent paths alongside it are left untouched (neither
    /// restored from the index nor unlinked).
    #[test]
    fn discard_submodule_internal_path_rejects_whole_batch() {
        let (dir, _child) = submodule_fixture("discard-sub-batch");
        commit_file(dir.path(), "tracked.txt", "clean\n");
        write_file(dir.path(), "tracked.txt", "dirty\n");
        write_file(dir.path(), "untracked.txt", "new\n");

        let err = discard(
            dir.path(),
            &[
                "tracked.txt".to_string(),
                "sub/a.txt".to_string(),
                "untracked.txt".to_string(),
            ],
        )
        .unwrap_err();
        assert!(
            format!("{err}").contains("is in submodule"),
            "unexpected error: {err}"
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("tracked.txt")).unwrap(),
            "dirty\n",
            "tracked path must not be restored when the batch is refused"
        );
        assert!(
            dir.path().join("untracked.txt").exists(),
            "untracked path must not be deleted when the batch is refused"
        );
        assert_submodule_intact(dir.path());
    }

    /// The gitlink path itself stays allowed: `git checkout -- sub` is a
    /// benign no-op that leaves the submodule's worktree (and its uncommitted
    /// edit) alone.
    #[test]
    fn discard_gitlink_path_itself_is_a_benign_noop() {
        let (dir, _child) = submodule_fixture("discard-sub-gitlink");
        discard(dir.path(), &["sub".to_string()]).unwrap();
        assert_submodule_intact(dir.path());
    }

    /// Regression (`./` bypass of the monorepo#1733 guard): a leading `./` is
    /// lexical noise that `Path::components` preserves, so the guard's
    /// component-wise prefix match missed `./sub/a.txt`, `discard` classified
    /// it as untracked and unlinked the submodule's uncommitted edit.
    #[test]
    fn discard_rejects_dot_slash_submodule_internal_path() {
        let (dir, _child) = submodule_fixture("discard-sub-dotslash");
        let err = discard(dir.path(), &["./sub/a.txt".to_string()]).unwrap_err();
        assert!(
            format!("{err}").contains("is in submodule"),
            "unexpected error: {err}"
        );
        assert_submodule_intact(dir.path());
    }

    /// The same bypass with the `./` deeper in the path, and with a doubled
    /// separator — both name the same file and must be refused.
    #[test]
    fn discard_rejects_interior_dot_and_double_slash_submodule_paths() {
        let (dir, _child) = submodule_fixture("discard-sub-interior");
        for spelling in ["sub/./a.txt", "sub//a.txt", ".///sub/./a.txt"] {
            let err = discard(dir.path(), &[spelling.to_string()]).unwrap_err();
            assert!(
                format!("{err}").contains("is in submodule"),
                "unexpected error for {spelling}: {err}"
            );
            assert_submodule_intact(dir.path());
        }
    }

    /// The worst case of the `./` bypass: `./sub` names the gitlink itself, so
    /// the guard allows it — but the un-normalized spelling missed the index
    /// probe, classified as untracked, and `remove_dir_all` wiped the WHOLE
    /// submodule working copy. It must stay a benign no-op like `sub`.
    #[test]
    fn discard_dot_slash_gitlink_never_removes_submodule_worktree() {
        let (dir, _child) = submodule_fixture("discard-sub-dotslash-gitlink");
        discard(dir.path(), &["./sub".to_string()]).unwrap();
        assert!(
            dir.path().join("sub").join(".git").exists(),
            "submodule working copy must not be removed"
        );
        assert_submodule_intact(dir.path());
    }

    /// Regression (trailing-separator bypass): `Path::components` folds a
    /// trailing `/` and a trailing `.`, so the guard saw an empty remainder and
    /// allowed the pathspec as "the gitlink itself" — while the classifier
    /// probed the index with the raw spelling, missed, and `remove_dir_all`
    /// wiped the whole submodule working copy including its `.git`. Every
    /// spelling of the gitlink must stay the same benign no-op as `sub`.
    #[test]
    fn discard_trailing_slash_gitlink_never_removes_submodule_worktree() {
        let (dir, _child) = submodule_fixture("discard-sub-trailing");
        for spelling in ["sub/", "sub/.", "sub/./", "./sub/"] {
            discard(dir.path(), &[spelling.to_string()]).unwrap();
            assert!(
                dir.path().join("sub").join(".git").exists(),
                "submodule working copy must survive spelling {spelling}"
            );
            assert_submodule_intact(dir.path());
        }
    }

    /// Regression (unconditional gitlink allow): a submodule staged for removal
    /// (`git rm --cached sub`) is still registered via `.gitmodules`, so the
    /// guard waved the gitlink path through — but with no stage-0 index entry
    /// the classifier routed it to `remove_dir_all`, deleting the whole
    /// submodule checkout including its `.git`. Real
    /// `git checkout -- sub` errors and touches nothing.
    #[test]
    fn discard_gitlink_missing_from_index_is_refused() {
        let (dir, _child) = submodule_fixture("discard-sub-uncached");
        {
            let repo = Repository::open(dir.path()).unwrap();
            let mut index = repo.index().unwrap();
            index.remove_path(Path::new("sub")).unwrap();
            index.write().unwrap();
        }
        for spelling in ["sub", "sub/", "./sub"] {
            let err = discard(dir.path(), &[spelling.to_string()]).unwrap_err();
            assert!(
                format!("{err}").contains("did not match any files"),
                "unexpected error for {spelling}: {err}"
            );
            assert_eq!(
                std::fs::read_to_string(dir.path().join("sub").join("a.txt")).unwrap(),
                "uncommitted\n",
                "submodule working-copy file must survive spelling {spelling}"
            );
            assert!(
                dir.path().join("sub").join(".git").exists(),
                "submodule git dir must survive spelling {spelling}"
            );
        }
    }

    /// An *ancestor* directory pathspec is intentionally not refused by the
    /// guard (the submodule is strictly inside it, not the other way round);
    /// it is kept safe by the tracked-subtree routing, which sends it through
    /// `checkout_index` instead of `remove_dir_all`. Pins that interaction so a
    /// later change to `index_has_dir_prefix` cannot silently turn it into a
    /// submodule-nuking deletion.
    #[test]
    fn discard_parent_directory_of_submodule_leaves_it_intact() {
        use crate::testutil::add_submodule;
        let child = init_repo("discard-sub-parentdir-child");
        commit_file(child.path(), "a.txt", "a\n");
        let dir = init_repo("discard-sub-parentdir-parent");
        commit_file(dir.path(), "seed.txt", "seed\n");
        add_submodule(dir.path(), child.path(), "packages/intentd");
        write_file(
            &dir.path().join("packages").join("intentd"),
            "a.txt",
            "uncommitted\n",
        );

        for spelling in ["packages", "packages/", "./packages"] {
            discard(dir.path(), &[spelling.to_string()]).unwrap();
            assert_eq!(
                std::fs::read_to_string(dir.path().join("packages").join("intentd").join("a.txt"))
                    .unwrap(),
                "uncommitted\n",
                "submodule edit must survive spelling {spelling}"
            );
            assert!(
                dir.path()
                    .join("packages")
                    .join("intentd")
                    .join(".git")
                    .exists(),
                "submodule git dir must survive spelling {spelling}"
            );
            let repo = Repository::open(dir.path()).unwrap();
            let index = repo.index().unwrap();
            assert_eq!(
                index
                    .get_path(Path::new("packages/intentd"), 0)
                    .unwrap()
                    .mode,
                u32::from(git2::FileMode::Commit),
                "gitlink entry intact for spelling {spelling}"
            );
        }
    }

    /// `stage` shares the guard, so the `./` spelling is refused there too and
    /// no submodule-internal blob reaches the index.
    #[test]
    fn stage_rejects_dot_slash_submodule_internal_path() {
        let (dir, _child) = submodule_fixture("stage-sub-dotslash");
        let err = stage(dir.path(), &["./sub/a.txt".to_string()]).unwrap_err();
        assert!(
            format!("{err}").contains("is in submodule"),
            "unexpected error: {err}"
        );
        let repo = Repository::open(dir.path()).unwrap();
        let index = repo.index().unwrap();
        assert!(
            !index
                .iter()
                .any(|e| String::from_utf8_lossy(&e.path).starts_with("sub/")),
            "no submodule-internal blob may be in the index"
        );
        assert_submodule_intact(dir.path());
    }

    /// A single-line unified-diff patch that turns the seed file's content
    /// from `"seed\n"` into `"seed\nnew line\n"`, safe to `git apply --cached`.
    fn append_line_patch(rel: &str) -> String {
        format!(
            "diff --git a/{rel} b/{rel}\n--- a/{rel}\n+++ b/{rel}\n@@ -1 +1,2 @@\n seed\n+new line\n"
        )
    }

    #[test]
    fn stage_hunk_stages_only_the_hunk() {
        let dir = init_repo("stage-hunk");
        commit_file(dir.path(), "seed.txt", "seed\n");
        // Working tree has the addition; the patch we apply matches it.
        write_file(dir.path(), "seed.txt", "seed\nnew line\n");
        let patch = append_line_patch("seed.txt");
        stage_hunk(dir.path(), "seed.txt", &patch).unwrap();
        let st = status(dir.path()).unwrap();
        let f = st.files.iter().find(|f| f.path == "seed.txt").unwrap();
        assert!(f.staged);
        assert_eq!(f.status, GitFileStatus::Modified);
    }

    #[test]
    fn unstage_hunk_reverses_a_staged_hunk() {
        let dir = init_repo("unstage-hunk");
        commit_file(dir.path(), "seed.txt", "seed\n");
        write_file(dir.path(), "seed.txt", "seed\nnew line\n");
        stage(dir.path(), &["seed.txt".to_string()]).unwrap();
        // Reverse the same hunk out of the index.
        let patch = append_line_patch("seed.txt");
        unstage_hunk(dir.path(), "seed.txt", &patch).unwrap();
        let st = status(dir.path()).unwrap();
        let f = st.files.iter().find(|f| f.path == "seed.txt").unwrap();
        assert!(!f.staged);
    }

    #[test]
    fn stage_hunk_invalid_patch_errors() {
        let dir = init_repo("stage-hunk-bad");
        commit_file(dir.path(), "seed.txt", "seed\n");
        // A patch that is well-formed at the header level but does not apply
        // (nothing to match) still surfaces as `Internal` (`-32603`) from
        // `git apply`.
        let unappliable = format!(
            "{}--- a/seed.txt\n+++ b/seed.txt\n@@ -1 +1 @@\n-nope\n+other\n",
            "diff --git a/seed.txt b/seed.txt\n"
        );
        let err = stage_hunk(dir.path(), "seed.txt", &unappliable).unwrap_err();
        assert!(matches!(err, Error::Internal(_)));
    }

    #[test]
    fn stage_hunk_rejects_missing_header() {
        let dir = init_repo("stage-hunk-noheader");
        commit_file(dir.path(), "seed.txt", "seed\n");
        let err = stage_hunk(dir.path(), "seed.txt", "not a patch\n").unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)));
    }

    #[test]
    fn stage_hunk_rejects_mismatched_path() {
        let dir = init_repo("stage-hunk-mismatch");
        commit_file(dir.path(), "seed.txt", "seed\n");
        // Header targets `other.txt` but caller claims `seed.txt`.
        let patch = append_line_patch("other.txt");
        let err = stage_hunk(dir.path(), "seed.txt", &patch).unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)));
    }

    #[test]
    fn stage_hunk_rejects_multi_file_patch() {
        let dir = init_repo("stage-hunk-multi");
        commit_file(dir.path(), "seed.txt", "seed\n");
        let mut patch = append_line_patch("seed.txt");
        patch.push_str(&append_line_patch("other.txt"));
        let err = stage_hunk(dir.path(), "seed.txt", &patch).unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)));
    }

    /// Regression: a path whose own name contains ` b/` (a directory literally
    /// named `dir b`) must still be accepted. Any `split_once(" b/")` /
    /// `rsplit_once(" b/")` header parser is ambiguous here — the header
    /// `a/dir b/file.txt b/dir b/file.txt` has three ` b/` occurrences and no
    /// single split (first, last, or middle) picks the right one. The current
    /// validator sidesteps the parse entirely by comparing the header against
    /// the synthesized `a/{path} b/{path}` string, so this path must round-trip
    /// cleanly.
    #[test]
    fn stage_hunk_accepts_path_containing_b_slash() {
        let dir = init_repo("stage-hunk-b-slash");
        let rel = "dir b/file.txt";
        commit_file(dir.path(), rel, "seed\n");
        write_file(dir.path(), rel, "seed\nnew line\n");
        let patch = append_line_patch(rel);
        stage_hunk(dir.path(), rel, &patch).unwrap();
        let st = status(dir.path()).unwrap();
        let f = st.files.iter().find(|f| f.path == rel).unwrap();
        assert!(f.staged);
        assert_eq!(f.status, GitFileStatus::Modified);
    }
}
