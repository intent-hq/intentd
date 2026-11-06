//! Git checks for the daemon's file-attribution bookkeeping
//! (`tracked_changes`, intent-hq/intent#7266).
//!
//! An attribution row only means something while its path is a change git can
//! stage or commit. Each check below is answered by the repository that
//! actually owns the path — a file inside a submodule is governed by the
//! submodule's own `.gitignore` and index, which the superproject's status
//! never reports:
//!
//! - [`is_ignored_untracked`]: should a fresh agent write be recorded at all?
//!   An untracked path matching ignore rules can never be committed.
//! - [`classify_paths`]: is an existing row's path still dirty? A row whose
//!   path is clean (committed, reverted, ignored, deleted-untracked) will never
//!   be moved out of its working stage by the commit pipeline.
//! - [`lockable_paths`]: which tracked paths are still changes, judged from an
//!   already-scanned (cached) superproject status — for the agent-lock filter.
//!
//! Submodules are located by looking up each path prefix as a gitlink in the
//! owning repository's index or `HEAD` tree — a few indexed lookups per path,
//! never a walk of the whole tree.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use git2::{Index, Oid, Repository};
use intent_core::{FileStatus, Result};

use crate::map_git_err;
use crate::submodule::{ignores_case, MAX_SUBMODULE_NESTING};

/// Tree-entry / index mode of a gitlink (submodule pin).
const GITLINK_MODE: u32 = 0o160_000;

/// One opened repository with the state the checks consult.
struct RepoView {
    repo: Repository,
    index: Option<Index>,
    head_tree: Option<Oid>,
    ignore_case: bool,
}

impl RepoView {
    fn open(workdir: &Path) -> Option<Self> {
        let repo = Repository::open(workdir).ok()?;
        let index = repo.index().ok();
        let head_tree = repo
            .head()
            .ok()
            .and_then(|h| h.peel_to_tree().ok())
            .map(|t| t.id());
        let ignore_case = ignores_case(&repo);
        Some(Self {
            repo,
            index,
            head_tree,
            ignore_case,
        })
    }

    /// The index mode and `HEAD` tree mode of `rel`, `None` where absent.
    fn modes(&self, rel: &str) -> (Option<u32>, Option<u32>) {
        let path = Path::new(rel);
        let in_index = self
            .index
            .as_ref()
            .and_then(|i| i.get_path(path, 0))
            .map(|e| e.mode);
        let in_head = self
            .head_tree
            .and_then(|oid| self.repo.find_tree(oid).ok())
            .and_then(|t| t.get_path(path).ok())
            .and_then(|e| u32::try_from(e.filemode()).ok());
        (in_index, in_head)
    }

    fn is_gitlink(&self, rel: &str) -> bool {
        let (index, head) = self.modes(rel);
        index == Some(GITLINK_MODE) || head == Some(GITLINK_MODE)
    }

    fn is_tracked(&self, rel: &str) -> bool {
        let (index, head) = self.modes(rel);
        index.is_some() || head.is_some()
    }

    fn is_ignored_untracked(&self, rel: &str) -> bool {
        !rel.is_empty()
            && self.repo.is_path_ignored(Path::new(rel)).unwrap_or(false)
            && !self.is_tracked(rel)
    }
}

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

/// The `/`-separated components of a normalized relative path, or `None` when
/// it is empty or escapes (`..`, absolute).
fn components(rel: &str) -> Option<Vec<&str>> {
    if rel.starts_with('/') {
        return None;
    }
    let parts: Vec<&str> = rel
        .split('/')
        .filter(|c| !c.is_empty() && *c != ".")
        .collect();
    if parts.is_empty() || parts.contains(&"..") {
        return None;
    }
    Some(parts)
}

/// Whether `rel` (inside `workdir`) lies under an untracked nested git
/// repository — a work tree the owning repository's status reports only as
/// an opaque `dir/` entry, so its files are never listed individually.
fn inside_nested_repo(workdir: &Path, rel: &str) -> bool {
    let Some(parts) = components(rel) else {
        return false;
    };
    let mut dir = workdir.to_path_buf();
    for part in &parts[..parts.len() - 1] {
        dir.push(part);
        if dir.join(".git").exists() {
            return true;
        }
    }
    false
}

/// Resolves worktree-relative paths to their owning repository, opening each
/// repository once.
struct Resolver {
    worktree: PathBuf,
    views: HashMap<PathBuf, Option<RepoView>>,
}

impl Resolver {
    fn new(worktree: &Path) -> Self {
        Self {
            worktree: worktree.to_path_buf(),
            views: HashMap::new(),
        }
    }

    fn view(&mut self, workdir: &Path) -> Option<&RepoView> {
        self.views
            .entry(workdir.to_path_buf())
            .or_insert_with(|| RepoView::open(workdir))
            .as_ref()
    }

    /// The innermost repository under the worktree that owns `rel_path`,
    /// descending through submodules (a path prefix that is a gitlink in the
    /// current repository's index or `HEAD`). `None` when the path is unsafe
    /// or a repository on the way cannot be opened (unpopulated submodule) —
    /// the caller cannot judge the path and must leave it alone.
    fn owner(&mut self, rel_path: &str) -> Option<Owner> {
        let mut workdir = self.worktree.clone();
        let mut prefix = String::new();
        let mut inner: Vec<String> = components(rel_path)?
            .into_iter()
            .map(str::to_string)
            .collect();
        for _ in 0..=MAX_SUBMODULE_NESTING {
            let view = self.view(&workdir)?;
            let split = (1..inner.len()).find(|&n| view.is_gitlink(&inner[..n].join("/")));
            let Some(n) = split else {
                return Some(Owner {
                    prefix,
                    workdir,
                    inner: inner.join("/"),
                });
            };
            let sm = inner[..n].join("/");
            workdir = workdir.join(&sm);
            prefix = if prefix.is_empty() {
                sm
            } else {
                format!("{prefix}/{sm}")
            };
            inner.drain(..n);
        }
        None
    }
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
    let mut resolver = Resolver::new(worktree);
    let Some(owner) = resolver.owner(rel_path) else {
        return false;
    };
    resolver
        .view(&owner.workdir)
        .is_some_and(|v| v.is_ignored_untracked(&owner.inner))
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
    /// submodule, failed submodule scan), or the path is inside an untracked
    /// nested repository; the path must be left alone.
    Unknown,
}

/// Classify each of `rel_paths` (relative to `worktree`) as [`PathState`],
/// scanning each owning repository's status once (rename detection off, so
/// both sides of a rename count as dirty). Paths inside a populated submodule
/// are judged by that submodule's status; comparisons fold case when the
/// owning repository sets `core.ignorecase`.
///
/// # Errors
///
/// Returns `Error::Internal` when the top-level repository cannot be opened
/// or scanned — the caller must then treat every path as unknown.
pub fn classify_paths(worktree: &Path, rel_paths: &[String]) -> Result<Vec<PathState>> {
    // Fail loudly when the superproject itself is unreadable: a sweep must
    // never treat "could not look" as "clean".
    let top_ignore_case = ignores_case(&Repository::open(worktree).map_err(map_git_err)?);
    let top = dirty_set(worktree, top_ignore_case)?;
    let mut scans: HashMap<String, Option<HashSet<String>>> = HashMap::new();
    scans.insert(String::new(), Some(top));
    let mut resolver = Resolver::new(worktree);
    let mut out = Vec::with_capacity(rel_paths.len());
    for path in rel_paths {
        let Some(owner) = resolver.owner(path) else {
            out.push(PathState::Unknown);
            continue;
        };
        let Some(view) = resolver.view(&owner.workdir) else {
            out.push(PathState::Unknown);
            continue;
        };
        let ignore_case = view.ignore_case;
        let set = scans
            .entry(owner.prefix.clone())
            .or_insert_with(|| dirty_set(&owner.workdir, ignore_case).ok());
        let Some(set) = set else {
            out.push(PathState::Unknown);
            continue;
        };
        out.push(if set.contains(&fold(&owner.inner, ignore_case)) {
            PathState::Dirty
        } else if view.is_ignored_untracked(&owner.inner) {
            PathState::Clean
        } else if inside_nested_repo(&owner.workdir, &owner.inner) {
            PathState::Unknown
        } else {
            PathState::Clean
        });
    }
    Ok(out)
}

fn fold(path: &str, ignore_case: bool) -> String {
    if ignore_case {
        path.to_lowercase()
    } else {
        path.to_string()
    }
}

/// Every path git reports as changed in the repository at `workdir`, rename
/// detection off, case-folded when `ignore_case`.
fn dirty_set(workdir: &Path, ignore_case: bool) -> Result<HashSet<String>> {
    Ok(crate::status::files_without_renames(workdir)?
        .into_iter()
        .map(|f| fold(&f.path, ignore_case))
        .collect())
}

/// Of `rel_paths` (relative to `worktree`), the ones that are still changes
/// according to `status` — an already-scanned, rename-detecting superproject
/// status (the daemon's status cache), so this does no status scan itself:
///
/// - paths `status` lists (case-folded under `core.ignorecase`);
/// - paths inside a submodule whose gitlink `status` lists as changed (the
///   superproject never lists the files inside a submodule);
/// - tracked paths missing on disk — the old side of a detected rename,
///   which a rename-detecting status folds into the new path's entry;
/// - paths inside an untracked nested repository, whose files the status
///   cannot list.
///
/// When the repository cannot be opened every path is returned, so a caller
/// filtering on the result degrades to keeping everything.
#[must_use]
pub fn lockable_paths(
    worktree: &Path,
    status: &[FileStatus],
    rel_paths: &[String],
) -> HashSet<String> {
    let listed: HashSet<&str> = status.iter().map(|f| f.path.as_str()).collect();
    let (mut out, unlisted): (HashSet<String>, Vec<String>) = {
        let (hit, miss): (Vec<&String>, Vec<&String>) =
            rel_paths.iter().partition(|p| listed.contains(p.as_str()));
        (
            hit.into_iter().cloned().collect(),
            miss.into_iter().cloned().collect(),
        )
    };
    if unlisted.is_empty() {
        return out;
    }
    let Some(view) = RepoView::open(worktree) else {
        out.extend(unlisted);
        return out;
    };
    let folded: HashSet<String> = if view.ignore_case {
        listed.iter().map(|p| p.to_lowercase()).collect()
    } else {
        HashSet::new()
    };
    let dirty_gitlinks: Vec<&str> = status
        .iter()
        .filter(|f| f.mode.as_deref() == Some("160000"))
        .map(|f| f.path.as_str())
        .collect();
    for path in unlisted {
        let submodule = components(&path).and_then(|parts| {
            (1..parts.len())
                .map(|n| parts[..n].join("/"))
                .find(|prefix| view.is_gitlink(prefix))
        });
        let keep = if let Some(submodule) = submodule {
            dirty_gitlinks.contains(&submodule.as_str())
        } else {
            (view.ignore_case && folded.contains(&path.to_lowercase()))
                || if std::fs::symlink_metadata(worktree.join(&path)).is_err() {
                    view.is_tracked(&path)
                } else {
                    !view.is_ignored_untracked(&path) && inside_nested_repo(worktree, &path)
                }
        };
        if keep {
            out.insert(path);
        }
    }
    out
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
        assert!(is_ignored_untracked(dir.path(), "./build.log"));
        write_file(dir.path(), ".dev/x.json", "{}");
        assert!(is_ignored_untracked(dir.path(), ".dev/x.json"));
    }

    #[test]
    fn non_ignored_paths_are_not_ignored() {
        let dir = init_repo("attr-not-ignored");
        commit_file(dir.path(), ".gitignore", ".dev/\n*.log\n");
        commit_file(dir.path(), "src/a.rs", "a\n");
        assert!(!is_ignored_untracked(dir.path(), "src/a.rs"));
        assert!(!is_ignored_untracked(dir.path(), "src/new.rs"));
        assert!(!is_ignored_untracked(dir.path(), "dev/x.json"));
        assert!(!is_ignored_untracked(dir.path(), "../escape.log"));
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
        use PathState::{Clean, Dirty};
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

    #[test]
    fn classify_leaves_paths_in_an_untracked_nested_repo_alone() {
        let dir = init_repo("attr-nested");
        commit_file(dir.path(), ".gitignore", "scratch/\n");
        commit_file(dir.path(), "README.md", "r\n");
        let nested = dir.path().join("vendor/tool");
        std::fs::create_dir_all(&nested).unwrap();
        Repository::init(&nested).unwrap();
        write_file(dir.path(), "vendor/tool/src/x.rs", "x\n");
        let ignored_nested = dir.path().join("scratch/clone");
        std::fs::create_dir_all(&ignored_nested).unwrap();
        Repository::init(&ignored_nested).unwrap();
        write_file(dir.path(), "scratch/clone/y.rs", "y\n");
        let rows = paths(&["vendor/tool/src/x.rs", "scratch/clone/y.rs"]);
        let got = classify_paths(dir.path(), &rows).unwrap();
        assert_eq!(got, vec![PathState::Unknown, PathState::Clean]);
        let status = crate::status::status(dir.path()).unwrap().files;
        assert_eq!(
            lockable_paths(dir.path(), &status, &rows),
            HashSet::from(["vendor/tool/src/x.rs".to_string()])
        );
    }

    #[test]
    fn classify_folds_case_when_the_repo_ignores_case() {
        let dir = init_repo("attr-case");
        commit_file(dir.path(), "README.md", "r\n");
        write_file(dir.path(), "README.md", "changed\n");
        Repository::open(dir.path())
            .unwrap()
            .config()
            .unwrap()
            .set_bool("core.ignorecase", true)
            .unwrap();
        let rows = paths(&["readme.md"]);
        assert_eq!(
            classify_paths(dir.path(), &rows).unwrap(),
            vec![PathState::Dirty]
        );
        let status = crate::status::status(dir.path()).unwrap().files;
        assert_eq!(
            lockable_paths(dir.path(), &status, &rows),
            HashSet::from(["readme.md".to_string()])
        );
    }

    #[test]
    fn resolver_opens_each_repository_once() {
        let child = init_repo("attr-resolver-child");
        commit_file(child.path(), "lib.rs", "x\n");
        let parent = init_repo("attr-resolver-parent");
        commit_file(parent.path(), "README.md", "r\n");
        add_submodule(parent.path(), child.path(), "sub");
        let mut resolver = Resolver::new(parent.path());
        for i in 0..500 {
            let top = resolver.owner(&format!("dir/f{i}.rs")).unwrap();
            assert_eq!(
                (top.prefix.as_str(), top.inner.as_str()),
                ("", &*format!("dir/f{i}.rs"))
            );
            let sub = resolver.owner(&format!("sub/g{i}.rs")).unwrap();
            assert_eq!(sub.prefix, "sub");
            assert_eq!(sub.inner, format!("g{i}.rs"));
        }
        assert_eq!(resolver.views.len(), 2, "one view per repository");
    }

    fn status_of(dir: &Path) -> Vec<FileStatus> {
        crate::status::status(dir).unwrap().files
    }

    #[test]
    fn lockable_keeps_listed_paths_and_the_old_side_of_a_rename() {
        let dir = init_repo("attr-lock-rename");
        commit_file(dir.path(), ".gitignore", ".dev/\n");
        commit_file(
            dir.path(),
            "old.rs",
            "fn main() { println!(\"unchanged body\"); }\n",
        );
        commit_file(dir.path(), "clean.rs", "c\n");
        std::fs::rename(dir.path().join("old.rs"), dir.path().join("new.rs")).unwrap();
        write_file(dir.path(), ".dev/x.json", "{}");
        let got = lockable_paths(
            dir.path(),
            &status_of(dir.path()),
            &paths(&["old.rs", "new.rs", "clean.rs", ".dev/x.json", "gone.rs"]),
        );
        assert_eq!(
            got,
            HashSet::from(["old.rs".to_string(), "new.rs".to_string()])
        );
    }

    #[test]
    fn lockable_keeps_submodule_paths_only_while_the_submodule_is_dirty() {
        let child = init_repo("attr-lock-sub-child");
        commit_file(child.path(), "lib.rs", "x\n");
        let parent = init_repo("attr-lock-sub-parent");
        commit_file(parent.path(), "README.md", "r\n");
        add_submodule(parent.path(), child.path(), "sub");
        let rows = paths(&["sub/lib.rs"]);
        assert!(lockable_paths(parent.path(), &status_of(parent.path()), &rows).is_empty());
        write_file(parent.path(), "sub/lib.rs", "changed\n");
        assert_eq!(
            lockable_paths(parent.path(), &status_of(parent.path()), &rows),
            HashSet::from(["sub/lib.rs".to_string()])
        );
    }

    #[test]
    fn lockable_keeps_everything_when_the_repo_is_unreadable() {
        let dir = init_repo("attr-lock-norepo");
        std::fs::remove_dir_all(dir.path().join(".git")).unwrap();
        assert_eq!(
            lockable_paths(dir.path(), &[], &paths(&["x"])),
            HashSet::from(["x".to_string()])
        );
    }
}
