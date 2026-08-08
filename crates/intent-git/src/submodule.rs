//! Submodule operations.
//!
//! After a pull that updates a submodule gitlink, the submodule worktree must
//! be synced to match the new commit. This module provides the bounded shell-out
//! pattern for `git submodule update --init --recursive`, following the same
//! approach as `fetch.rs` (timeout + kill, GIT_TERMINAL_PROMPT=0, piped stderr).

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use git2::{FileMode, Repository, TreeWalkMode, TreeWalkResult};
use intent_core::{Error, Result};

use crate::map_git_err;

/// Wall-clock bound for `git submodule update`. Chosen below the service-layer
/// `GIT_PULL_TIMEOUT` (120s) so the submodule-update child is killed cleanly
/// by this helper before the outer timeout wrapper fires.
const SUBMODULE_UPDATE_TIMEOUT: Duration = Duration::from_secs(100);

/// Poll interval used while waiting for the submodule-update child to exit.
const SUBMODULE_UPDATE_POLL: Duration = Duration::from_millis(50);

/// Update submodules to match the current gitlinks (the post-pull step when
/// a repo has configured submodules). Shells out to `git submodule update
/// --init --recursive` with the same bounded pattern as `fetch.rs`:
/// `GIT_TERMINAL_PROMPT=0` for fail-fast, stdin null, stdout discarded, stderr
/// piped, wall-clock deadline + `Child::kill`. Errors when git is not on PATH,
/// a submodule is unreachable, or the update exceeds the timeout.
pub fn update_submodules(worktree_path: &Path) -> Result<()> {
    update_submodules_with_timeout(worktree_path, SUBMODULE_UPDATE_TIMEOUT)
}

/// Timeout-parameterised body of [`update_submodules`], factored out so tests
/// can drive the deadline-kill path without waiting 100s.
pub(crate) fn update_submodules_with_timeout(
    worktree_path: &Path,
    timeout: Duration,
) -> Result<()> {
    let mut child = Command::new("git")
        .arg("-C")
        .arg(worktree_path)
        .arg("submodule")
        .arg("update")
        .arg("--init")
        .arg("--recursive")
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        // Discard stdout: git-submodule progress output is only for TTY users.
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| Error::Internal(format!("failed to spawn git submodule update: {e}")))?;

    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if status.success() {
                    return Ok(());
                }
                let stderr = read_stderr(&mut child);
                return Err(Error::Internal(format!(
                    "git submodule update failed: {}",
                    stderr.trim()
                )));
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(Error::Internal(format!(
                        "git submodule update timed out after {}s",
                        timeout.as_secs()
                    )));
                }
                std::thread::sleep(SUBMODULE_UPDATE_POLL);
            }
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(Error::Internal(format!(
                    "git submodule update wait failed: {e}"
                )));
            }
        }
    }
}

/// Check whether a repository has configured submodules by testing for a
/// `.gitmodules` file in the worktree root.
pub fn has_submodules(worktree_path: &Path) -> bool {
    worktree_path.join(".gitmodules").exists()
}

/// The set of registered submodule (gitlink) paths in `repo`, worktree-relative
/// with forward-slash separators. Guards a `commit_paths_with_trailers`/`stage`
/// caller from ever flattening a submodule into the superproject (monorepo#1714):
/// a path strictly inside one of these is refused before it reaches
/// `index.add_path`, while the gitlink path itself (a pin bump) stays allowed.
///
/// Collected from three sources, unioned defensively since no single one is
/// complete on its own:
/// - the HEAD tree (filemode `160000` / [`FileMode::Commit`] entries) — covers
///   a submodule already committed even if `.gitmodules` was later deleted;
/// - the on-disk index (same filemode check) — covers a submodule staged for
///   removal or add that HEAD does not yet reflect;
/// - [`Repository::submodules`] (`.gitmodules` + config) — the backstop for a
///   submodule registered but not yet committed anywhere.
pub fn submodule_paths(repo: &Repository) -> Result<std::collections::BTreeSet<String>> {
    let mut paths = std::collections::BTreeSet::new();
    let commit_mode = i32::from(FileMode::Commit);

    if let Ok(head) = repo.head() {
        if let Some(oid) = head.target() {
            if let Ok(commit) = repo.find_commit(oid) {
                let tree = commit.tree().map_err(map_git_err)?;
                tree.walk(TreeWalkMode::PreOrder, |root, entry| {
                    if entry.filemode() == commit_mode {
                        if let Ok(name) = entry.name() {
                            paths.insert(format!("{root}{name}"));
                        }
                    }
                    TreeWalkResult::Ok
                })
                .map_err(map_git_err)?;
            }
        }
    }

    let index = repo.index().map_err(map_git_err)?;
    let commit_mode_u32 = u32::from(FileMode::Commit);
    for entry in index.iter() {
        if entry.mode == commit_mode_u32 {
            paths.insert(String::from_utf8_lossy(&entry.path).to_string());
        }
    }

    if let Ok(submodules) = repo.submodules() {
        for sm in submodules {
            paths.insert(sm.path().to_string_lossy().to_string());
        }
    }

    Ok(paths)
}

/// Whether `repo` treats paths case-insensitively (`core.ignorecase`, set by
/// `git init` on macOS/Windows filesystems). Callers pass the result to
/// [`submodule_containing`]/[`to_repo_relative`] so the gitlink guard compares
/// paths the same way the filesystem — and therefore `index.add_path` — does.
pub fn ignores_case(repo: &Repository) -> bool {
    repo.config()
        .and_then(|c| c.get_bool("core.ignorecase"))
        .unwrap_or(false)
}

/// Strip `prefix` from `target` component-by-component, returning the remainder
/// (empty when the two are equal) or `None` when `target` is not under
/// `prefix`. Component-wise (never byte-wise substring) so a sibling directory
/// sharing a prefix (`subdir/a.txt` vs `sub`) never matches; with `ignore_case`
/// the components are compared case-insensitively, which is what makes the
/// guard hold on a case-insensitive filesystem where `SUB/a.txt` and `sub/a.txt`
/// name the same file on disk.
fn strip_prefix_components(
    target: &Path,
    prefix: &Path,
    ignore_case: bool,
) -> Option<std::path::PathBuf> {
    let mut rest = target.components();
    let mut want = prefix.components();
    loop {
        match (want.next(), rest.clone().next()) {
            (None, _) => return Some(rest.as_path().to_path_buf()),
            (Some(_), None) => return None,
            (Some(w), Some(t)) => {
                let eq = if ignore_case {
                    w.as_os_str().to_string_lossy().to_lowercase()
                        == t.as_os_str().to_string_lossy().to_lowercase()
                } else {
                    w.as_os_str() == t.as_os_str()
                };
                if !eq {
                    return None;
                }
                rest.next();
            }
        }
    }
}

/// When `rel_path` lies strictly inside one of `submodules`, returns the
/// containing submodule path; `None` when `rel_path` names the submodule
/// itself (a pin-bump commit/stage target, always allowed) or is unrelated.
///
/// With `ignore_case` (the repo's `core.ignorecase`, see [`ignores_case`]) the
/// comparison folds case: on a case-insensitive filesystem a case-variant
/// spelling (`SUB/a.txt` for submodule `sub`) resolves to the same file on
/// disk, so a byte-exact comparison would let it past the guard and
/// `index.add_path` would flatten the `160000` gitlink into a tree.
pub fn submodule_containing<'a>(
    submodules: &'a std::collections::BTreeSet<String>,
    rel_path: &str,
    ignore_case: bool,
) -> Option<&'a str> {
    let target = Path::new(rel_path);
    for sm in submodules {
        if let Some(rest) = strip_prefix_components(target, Path::new(sm), ignore_case) {
            if !rest.as_os_str().is_empty() {
                return Some(sm.as_str());
            }
        }
    }
    None
}

/// Refuse any of `paths` that lies strictly inside a registered submodule,
/// with a message naming the offending path and its containing submodule
/// (parity with real `git add`'s "is in submodule" pathspec error). Callers
/// invoke this once, before any index mutation, so a rejected batch leaves
/// the index and HEAD untouched.
///
/// Each path is matched in its **repo-relative** form: registered submodule
/// paths are worktree-relative, so an in-worktree *absolute* path
/// (`/repo/sub/a.txt`) would otherwise miss the guard and still reach
/// `index.add_path` once the caller normalizes it — recreating the
/// gitlink-flattening bug (monorepo#1714). The error message keeps the
/// caller's original spelling.
///
/// Matching folds case when the repo has `core.ignorecase` set (the default on
/// macOS/Windows), where a case-variant spelling names the same file on disk.
pub fn reject_submodule_internal_paths(repo: &Repository, paths: &[String]) -> Result<()> {
    let submodules = submodule_paths(repo)?;
    if submodules.is_empty() {
        return Ok(());
    }
    let ignore_case = ignores_case(repo);
    let workdir = repo.workdir().map(Path::to_path_buf);
    for raw in paths {
        let rel = to_repo_relative(workdir.as_deref(), raw, ignore_case);
        if let Some(sm) = submodule_containing(&submodules, &rel, ignore_case) {
            return Err(Error::Internal(format!(
                "fatal: Pathspec '{raw}' is in submodule '{sm}'"
            )));
        }
    }
    Ok(())
}

/// Repo-relative form of `raw`: relative paths pass through unchanged, and an
/// absolute path is stripped of the worktree prefix. The strip is retried
/// against the canonicalized worktree so a symlinked worktree root (macOS
/// `/tmp` → `/private/tmp`) still matches without requiring `raw` to exist on
/// disk (a staged deletion names a missing path). An absolute path that is not
/// under the worktree is returned unchanged — it can never name a submodule
/// path, and the caller's own normalization leaves it alone too.
///
/// Public so the service-layer guard (`git.agentCommit`'s explicit `files`
/// rejection) matches submodule paths in the same normalized space instead of
/// duplicating the logic. `ignore_case` (the repo's `core.ignorecase`, see
/// [`ignores_case`]) folds case in the worktree-prefix comparison.
pub fn to_repo_relative(workdir: Option<&Path>, raw: &str, ignore_case: bool) -> String {
    let p = Path::new(raw);
    if !p.is_absolute() {
        return raw.to_string();
    }
    let Some(workdir) = workdir else {
        return raw.to_string();
    };
    if let Some(rest) = strip_prefix_components(p, workdir, ignore_case) {
        return rest.to_string_lossy().to_string();
    }
    if let (Ok(w), Some(full)) = (std::fs::canonicalize(workdir), canonical_ish(p)) {
        if let Some(rest) = strip_prefix_components(&full, &w, ignore_case) {
            return rest.to_string_lossy().to_string();
        }
    }
    raw.to_string()
}

/// Canonicalize `p`, falling back to canonicalizing its parent and re-joining
/// the file name so a path that does not exist on disk (a staged deletion)
/// still resolves its symlinked ancestry.
fn canonical_ish(p: &Path) -> Option<std::path::PathBuf> {
    if let Ok(full) = std::fs::canonicalize(p) {
        return Some(full);
    }
    let parent = p.parent()?;
    let name = p.file_name()?;
    std::fs::canonicalize(parent).ok().map(|c| c.join(name))
}

fn read_stderr(child: &mut std::process::Child) -> String {
    use std::io::Read;
    let mut buf = String::new();
    if let Some(mut stderr) = child.stderr.take() {
        let _ = stderr.read_to_string(&mut buf);
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{commit_file, init_repo};

    /// Update a repo with no submodules must succeed as a no-op.
    #[test]
    fn update_submodules_with_no_submodules_is_noop() {
        let dir = init_repo("submodule-update-noop");
        commit_file(dir.path(), "a.txt", "one\n");
        assert!(!has_submodules(dir.path()));
        update_submodules(dir.path()).unwrap();
    }

    /// Repos without .gitmodules report `has_submodules() == false`.
    #[test]
    fn has_submodules_false_when_no_gitmodules() {
        let dir = init_repo("no-submodules");
        commit_file(dir.path(), "a.txt", "x\n");
        assert!(!has_submodules(dir.path()));
    }

    /// A repo with a .gitmodules file reports `has_submodules() == true`.
    #[test]
    fn has_submodules_true_when_gitmodules_exists() {
        let dir = init_repo("with-gitmodules");
        commit_file(dir.path(), ".gitmodules", "[submodule \"test\"]\n");
        assert!(has_submodules(dir.path()));
    }

    fn submodules(paths: &[&str]) -> std::collections::BTreeSet<String> {
        paths.iter().map(|p| p.to_string()).collect()
    }

    /// A path strictly inside a submodule matches; the gitlink path itself
    /// (a pin bump) and an unrelated path do not.
    #[test]
    fn submodule_containing_matches_only_internal_paths() {
        let sms = submodules(&["sub"]);
        assert_eq!(submodule_containing(&sms, "sub/a.txt", false), Some("sub"));
        assert_eq!(
            submodule_containing(&sms, "sub/deep/a.txt", false),
            Some("sub")
        );
        assert_eq!(submodule_containing(&sms, "sub", false), None);
        assert_eq!(submodule_containing(&sms, "other/a.txt", false), None);
    }

    /// A sibling directory whose name merely starts with the submodule's name
    /// must not match — the comparison is component-wise, not a byte prefix.
    #[test]
    fn submodule_containing_ignores_sibling_prefix() {
        let sms = submodules(&["sub"]);
        assert_eq!(submodule_containing(&sms, "subdir/a.txt", false), None);
        assert_eq!(submodule_containing(&sms, "subdir", false), None);
        assert_eq!(submodule_containing(&sms, "subdir/a.txt", true), None);
    }

    /// Nested submodule paths match on their own full component prefix.
    #[test]
    fn submodule_containing_matches_nested_submodule() {
        let sms = submodules(&["packages/sub"]);
        assert_eq!(
            submodule_containing(&sms, "packages/sub/a.txt", false),
            Some("packages/sub")
        );
        assert_eq!(
            submodule_containing(&sms, "packages/subx/a.txt", false),
            None
        );
    }

    /// Regression (case-variant bypass): with `core.ignorecase` a spelling that
    /// differs only in case names the same file on disk, so it must be caught.
    /// Byte-exact matching (`ignore_case = false`) deliberately lets it through.
    #[test]
    fn submodule_containing_folds_case_when_requested() {
        let sms = submodules(&["sub"]);
        assert_eq!(submodule_containing(&sms, "SUB/a.txt", true), Some("sub"));
        assert_eq!(submodule_containing(&sms, "Sub/a.txt", true), Some("sub"));
        // The gitlink itself stays allowed under any spelling.
        assert_eq!(submodule_containing(&sms, "SUB", true), None);
        assert_eq!(submodule_containing(&sms, "SUB/a.txt", false), None);
    }

    /// Relative paths pass through; an absolute in-worktree path is stripped
    /// to its worktree-relative form.
    #[test]
    fn to_repo_relative_strips_worktree_prefix() {
        let dir = init_repo("to-repo-relative");
        let wd = dir.path();
        assert_eq!(to_repo_relative(Some(wd), "sub/a.txt", false), "sub/a.txt");
        let abs = wd.join("sub").join("a.txt");
        assert_eq!(
            to_repo_relative(Some(wd), &abs.to_string_lossy(), false),
            "sub/a.txt"
        );
        // No workdir known → unchanged.
        assert_eq!(
            to_repo_relative(None, &abs.to_string_lossy(), false),
            abs.to_string_lossy()
        );
    }

    /// The canonicalizing fallback resolves a symlinked worktree root even for
    /// a path that does not exist on disk (a staged deletion).
    #[test]
    fn to_repo_relative_canonicalizes_symlinked_worktree() {
        let dir = init_repo("to-repo-relative-symlink");
        std::fs::create_dir_all(dir.path().join("sub")).unwrap();
        let canonical = std::fs::canonicalize(dir.path()).unwrap();
        let abs = canonical.join("sub").join("gone.txt");
        assert_eq!(
            to_repo_relative(Some(dir.path()), &abs.to_string_lossy(), false),
            "sub/gone.txt"
        );
    }

    /// An absolute path outside the worktree cannot name a submodule path, so
    /// it is returned unchanged.
    #[test]
    fn to_repo_relative_leaves_outside_paths_unchanged() {
        let dir = init_repo("to-repo-relative-outside");
        let outside = std::env::temp_dir().join("intent-git-outside-xyz.txt");
        let raw = outside.to_string_lossy().to_string();
        assert_eq!(to_repo_relative(Some(dir.path()), &raw, false), raw);
    }

    /// With `core.ignorecase`, a case-variant absolute worktree prefix still
    /// strips — matching how the filesystem resolves it.
    #[test]
    fn to_repo_relative_folds_case_when_requested() {
        assert_eq!(
            to_repo_relative(Some(Path::new("/repo/Work")), "/repo/WORK/sub/a.txt", true),
            "sub/a.txt"
        );
        assert_eq!(
            to_repo_relative(Some(Path::new("/repo/Work")), "/repo/WORK/sub/a.txt", false),
            "/repo/WORK/sub/a.txt"
        );
    }

    /// `ignores_case` reflects the repo's `core.ignorecase` config.
    #[test]
    fn ignores_case_reads_repo_config() {
        let dir = init_repo("ignorecase-config");
        let repo = Repository::open(dir.path()).unwrap();
        repo.config()
            .unwrap()
            .set_bool("core.ignorecase", true)
            .unwrap();
        assert!(ignores_case(&repo));
        repo.config()
            .unwrap()
            .set_bool("core.ignorecase", false)
            .unwrap();
        assert!(!ignores_case(&repo));
    }
}
