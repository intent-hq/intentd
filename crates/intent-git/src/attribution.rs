//! Git checks for the daemon's file-attribution bookkeeping
//! (`tracked_changes`, intent-hq/intent#7266).
//!
//! An attribution row only means something while its path is a change git can
//! stage or commit. Two questions decide that, and both must be answered by the
//! repository that actually owns the path — a file inside a submodule is
//! governed by the submodule's own `.gitignore` and index, which the
//! superproject's status never reports:
//!
//! - [`is_ignored_untracked`]: should a fresh agent write be recorded at all?
//!   An untracked path matching ignore rules can never be committed.
//! - [`classify_paths`]: is an existing row's path still dirty? A row whose
//!   path is clean (committed, reverted, ignored, deleted-untracked) will never
//!   be moved out of its working stage by the commit pipeline.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use git2::Repository;
use intent_core::Result;

use crate::map_git_err;
use crate::submodule::{ignores_case, submodule_containing, submodule_paths, MAX_SUBMODULE_NESTING};

/// The repository that owns one worktree-relative path.
struct Owner {
    /// The owning repository's worktree-relative path (`""` for the
    /// superproject, `sub` / `sub/inner` for submodules).
    prefix: String,
    /// The owning repository's work tree on disk.
    workdir: PathBuf,
    /// The path relative to the owning repository.
    inner: String,
}

/// Resolve the innermost repository under `worktree` that owns `rel_path`,
/// descending through populated submodules. `None` when a containing
/// submodule cannot be opened (unpopulated, or an unsafe path) — the caller
/// cannot judge the path and must leave it alone.
fn owning_repo(worktree: &Path, rel_path: &str) -> Option<Owner> {
    let mut workdir = worktree.to_path_buf();
    let mut prefix = String::new();
    let mut inner = rel_path.to_string();
    for _ in 0..=MAX_SUBMODULE_NESTING {
        let repo = Repository::open(&workdir).ok()?;
        let submodules = submodule_paths(&repo).ok()?;
        let Some(sm) = submodule_containing(&submodules, &inner, ignores_case(&repo)) else {
            return Some(Owner {
                prefix,
                workdir,
                inner,
            });
        };
        let sm_rel = crate::repo_cache::safe_rel_path(sm)?;
        let rest = Path::new(&inner)
            .components()
            .filter(|c| !matches!(c, std::path::Component::CurDir))
            .skip(sm_rel.components().count())
            .collect::<PathBuf>()
            .to_string_lossy()
            .replace('\\', "/");
        let sm = sm.to_string();
        workdir = workdir.join(&sm_rel);
        prefix = if prefix.is_empty() {
            sm
        } else {
            format!("{prefix}/{sm}")
        };
        inner = rest;
    }
    None
}

/// Whether `rel_path` (relative to `worktree`) is an **untracked path that git
/// ignores** — the `git check-ignore` notion, which reports only paths absent
/// from the index. Such a path can never be staged or committed, so recording
/// agent attribution for it only accumulates rows nothing will ever clear.
///
/// A path git already tracks (in the index or in `HEAD`, e.g. force-added
/// under an ignored directory) is a real change and returns `false`. The
/// check runs against the repository that owns the path, so a submodule's own
/// `.gitignore` applies to paths inside it. Returns `false` whenever the
/// answer is unknown (not a repository, unpopulated submodule, libgit2 error)
/// — callers then keep recording as they always have.
#[must_use]
pub fn is_ignored_untracked(worktree: &Path, rel_path: &str) -> bool {
    let Some(owner) = owning_repo(worktree, rel_path) else {
        return false;
    };
    if owner.inner.is_empty() {
        return false;
    }
    let Ok(repo) = Repository::open(&owner.workdir) else {
        return false;
    };
    let inner = Path::new(&owner.inner);
    if !repo.is_path_ignored(inner).unwrap_or(false) {
        return false;
    }
    let in_index = repo
        .index()
        .ok()
        .is_some_and(|index| index.get_path(inner, 0).is_some());
    if in_index {
        return false;
    }
    let in_head = repo
        .head()
        .ok()
        .and_then(|h| h.peel_to_tree().ok())
        .is_some_and(|tree| tree.get_path(inner).is_ok());
    !in_head
}

/// Git's verdict on one worktree-relative path, from [`classify_paths`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathState {
    /// Staged, unstaged, or untracked (not ignored) change — including either
    /// side of a rename.
    Dirty,
    /// No change git would stage: unchanged against `HEAD`, ignored, or
    /// absent and untracked.
    Clean,
    /// The owning repository could not be opened or scanned (unpopulated
    /// submodule, failed submodule scan); the path must be left alone.
    Unknown,
}

/// Classify each of `rel_paths` (relative to `worktree`) as [`PathState`],
/// scanning each owning repository's status once (rename detection off, so
/// both sides of a rename count as dirty). Paths inside a populated submodule
/// are judged by that submodule's status; a submodule that cannot be opened
/// or scanned yields [`PathState::Unknown`] for its paths.
///
/// # Errors
///
/// Returns `Error::Internal` when the top-level repository cannot be opened
/// or scanned — the caller must then treat every path as unknown.
pub fn classify_paths(worktree: &Path, rel_paths: &[String]) -> Result<Vec<PathState>> {
    // Fail loudly when the superproject itself is unreadable: a sweep must
    // never treat "could not look" as "clean".
    Repository::open(worktree).map_err(map_git_err)?;
    let top = dirty_set(worktree)?;
    let mut scans: HashMap<String, Option<HashSet<String>>> = HashMap::new();
    scans.insert(String::new(), Some(top));
    let mut out = Vec::with_capacity(rel_paths.len());
    for path in rel_paths {
        let Some(owner) = owning_repo(worktree, path) else {
            out.push(PathState::Unknown);
            continue;
        };
        let set = scans
            .entry(owner.prefix.clone())
            .or_insert_with(|| dirty_set(&owner.workdir).ok());
        out.push(match set {
            Some(set) if set.contains(&owner.inner) => PathState::Dirty,
            Some(_) => PathState::Clean,
            None => PathState::Unknown,
        });
    }
    Ok(out)
}

/// Every path git reports as changed in the repository at `workdir`, rename
/// detection off.
fn dirty_set(workdir: &Path) -> Result<HashSet<String>> {
    Ok(crate::status::files_without_renames(workdir)?
        .into_iter()
        .map(|f| f.path)
        .collect())
}

/// The registered submodule paths of the repository at `worktree` (top level
/// only), or an empty set when it cannot be read. For callers that hold a
/// superproject-only status and must recognise paths it cannot see into.
#[must_use]
pub fn top_level_submodules(worktree: &Path) -> BTreeSet<String> {
    Repository::open(worktree)
        .ok()
        .and_then(|repo| submodule_paths(&repo).ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{add_submodule, commit_file, init_repo, write_file};

    #[test]
    fn ignored_untracked_path_is_ignored() {
        let dir = init_repo("attr-ignored");
        commit_file(dir.path(), ".gitignore", ".dev/\n*.log\n");
        assert!(is_ignored_untracked(dir.path(), ".dev/x.json"));
        assert!(is_ignored_untracked(dir.path(), ".dev/deep/nested/y.json"));
        assert!(is_ignored_untracked(dir.path(), "build.log"));
        write_file(dir.path(), ".dev/x.json", "{}");
        assert!(is_ignored_untracked(dir.path(), ".dev/x.json"));
    }

    #[test]
    fn non_ignored_paths_are_not_ignored() {
        let dir = init_repo("attr-not-ignored");
        commit_file(dir.path(), ".gitignore", ".dev/\n");
        commit_file(dir.path(), "src/a.rs", "a\n");
        assert!(!is_ignored_untracked(dir.path(), "src/a.rs"));
        assert!(!is_ignored_untracked(dir.path(), "src/new.rs"));
        assert!(!is_ignored_untracked(dir.path(), "dev/x.json"));
    }

    #[test]
    fn negated_rule_is_not_ignored() {
        let dir = init_repo("attr-negated");
        commit_file(dir.path(), ".gitignore", "*.json\n!keep.json\n");
        assert!(is_ignored_untracked(dir.path(), "drop.json"));
        assert!(!is_ignored_untracked(dir.path(), "keep.json"));
    }

    #[test]
    fn tracked_file_matching_ignore_rules_is_not_ignored() {
        let dir = init_repo("attr-force-added");
        commit_file(dir.path(), ".gitignore", ".dev/\n");
        // `commit_file` adds through the index directly, like `git add -f`.
        commit_file(dir.path(), ".dev/tracked.json", "{}");
        assert!(!is_ignored_untracked(dir.path(), ".dev/tracked.json"));
        std::fs::remove_file(dir.path().join(".dev/tracked.json")).unwrap();
        assert!(!is_ignored_untracked(dir.path(), ".dev/tracked.json"));
    }

    #[test]
    fn submodule_gitignore_governs_paths_inside_it() {
        let child = init_repo("attr-sub-child");
        commit_file(child.path(), ".gitignore", "target/\n");
        commit_file(child.path(), "lib.rs", "x\n");
        let parent = init_repo("attr-sub-parent");
        commit_file(parent.path(), "README.md", "r\n");
        add_submodule(parent.path(), child.path(), "pkg/sub");
        assert!(is_ignored_untracked(parent.path(), "pkg/sub/target/out.o"));
        assert!(!is_ignored_untracked(parent.path(), "pkg/sub/lib.rs"));
        assert!(!is_ignored_untracked(parent.path(), "pkg/sub/new.rs"));
    }

    #[test]
    fn non_repository_is_never_ignored() {
        let dir = init_repo("attr-no-repo");
        std::fs::remove_dir_all(dir.path().join(".git")).unwrap();
        assert!(!is_ignored_untracked(dir.path(), "anything.txt"));
        assert!(classify_paths(dir.path(), &["anything.txt".to_string()]).is_err());
    }

    fn paths(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn classify_separates_dirty_from_clean() {
        let dir = init_repo("attr-classify");
        commit_file(dir.path(), ".gitignore", ".dev/\n");
        commit_file(dir.path(), "kept.rs", "a\n");
        commit_file(dir.path(), "edited.rs", "a\n");
        commit_file(dir.path(), "removed.rs", "a\n");
        commit_file(dir.path(), "reverted.rs", "a\n");
        write_file(dir.path(), "edited.rs", "b\n");
        std::fs::remove_file(dir.path().join("removed.rs")).unwrap();
        write_file(dir.path(), "reverted.rs", "a\n");
        write_file(dir.path(), "untracked.rs", "n\n");
        write_file(dir.path(), ".dev/scratch.json", "{}");
        let got = classify_paths(
            dir.path(),
            &paths(&[
                "edited.rs",
                "removed.rs",
                "untracked.rs",
                "kept.rs",
                "reverted.rs",
                ".dev/scratch.json",
                ".dev/gone.json",
                "never-existed.rs",
            ]),
        )
        .unwrap();
        use PathState::{Clean, Dirty};
        assert_eq!(
            got,
            vec![Dirty, Dirty, Dirty, Clean, Clean, Clean, Clean, Clean]
        );
    }

    #[test]
    fn classify_counts_both_sides_of_a_staged_rename() {
        let dir = init_repo("attr-rename");
        commit_file(dir.path(), "old.rs", "fn main() {}\n");
        std::fs::rename(dir.path().join("old.rs"), dir.path().join("new.rs")).unwrap();
        let repo = Repository::open(dir.path()).unwrap();
        let mut index = repo.index().unwrap();
        index.remove_path(Path::new("old.rs")).unwrap();
        index.add_path(Path::new("new.rs")).unwrap();
        index.write().unwrap();
        let got = classify_paths(dir.path(), &paths(&["old.rs", "new.rs"])).unwrap();
        assert_eq!(got, vec![PathState::Dirty, PathState::Dirty]);
    }

    #[test]
    fn classify_judges_submodule_paths_by_the_submodule() {
        let child = init_repo("attr-csub-child");
        commit_file(child.path(), ".gitignore", "target/\n");
        commit_file(child.path(), "lib.rs", "x\n");
        commit_file(child.path(), "clean.rs", "c\n");
        let parent = init_repo("attr-csub-parent");
        commit_file(parent.path(), "README.md", "r\n");
        add_submodule(parent.path(), child.path(), "sub");
        write_file(parent.path(), "sub/lib.rs", "changed\n");
        write_file(parent.path(), "sub/target/out.o", "bin");
        let got = classify_paths(
            parent.path(),
            &paths(&["sub/lib.rs", "sub/clean.rs", "sub/target/out.o"]),
        )
        .unwrap();
        assert_eq!(
            got,
            vec![PathState::Dirty, PathState::Clean, PathState::Clean]
        );
        assert!(top_level_submodules(parent.path()).contains("sub"));
    }

    #[test]
    fn classify_reports_unpopulated_submodule_paths_as_unknown() {
        let child = init_repo("attr-usub-child");
        commit_file(child.path(), "lib.rs", "x\n");
        let parent = init_repo("attr-usub-parent");
        commit_file(parent.path(), "README.md", "r\n");
        add_submodule(parent.path(), child.path(), "sub");
        std::fs::remove_dir_all(parent.path().join("sub")).unwrap();
        std::fs::create_dir_all(parent.path().join("sub")).unwrap();
        let got = classify_paths(parent.path(), &paths(&["sub/lib.rs", "README.md"])).unwrap();
        assert_eq!(got, vec![PathState::Unknown, PathState::Clean]);
    }
}
