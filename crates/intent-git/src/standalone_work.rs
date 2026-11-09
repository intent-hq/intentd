//! Work that exists only inside a standalone checkout's folder (PROTOCOL
//! §5.1 "Automatic purge"): the safety gate for purging a `cow` / `direct`
//! workspace, whose folder IS the repository.
//!
//! A linked worktree's branch survives a purge in its source repository; a
//! standalone checkout's branches, stashes and nested repositories do not.
//! [`standalone_work`] therefore walks the whole folder the purge removes and
//! evaluates every git repository in it — the primary checkout, populated
//! submodules, nested clones and linked worktrees (such as `.dev/*`), and
//! bare repositories. Any of these blocks the purge:
//!
//! - uncommitted or untracked changes ([`StandaloneWorkKind::Uncommitted`]).
//!   A submodule entry is not itself a change: the submodule's own checkout
//!   is evaluated as a repository of its own, and a staged gitlink must name
//!   a commit some remote ref reaches;
//! - a commit reachable from `HEAD` or any local ref — branches, tags,
//!   notes, a remote agent's `refs/intent/...` work refs — but from no
//!   `refs/remotes/*` ref ([`StandaloneWorkKind::Unpushed`]). A merged PR
//!   head covering `HEAD` ([`crate::head_reached_by`]) lifts the commits
//!   `HEAD` reaches, never those only another ref reaches;
//! - a stash ([`StandaloneWorkKind::Stash`]);
//! - a linked worktree registered in a repository of the folder that lives
//!   outside it ([`StandaloneWorkKind::ExternalWorktree`]): removing the
//!   repository would orphan it.
//!
//! The walk never follows symlinks and skips `.git` directories (a
//! repository's `.git/modules/*` submodule repositories, deinitialized ones
//! included, are evaluated for stashes and unpushed commits), `node_modules`
//! and cache directories tagged with `CACHEDIR.TAG` (build output such as
//! Cargo's `target/`). Ignored files are not changes: they are lost with the
//! folder, as with any purge. Anything it cannot read or evaluate is an
//! error, which the caller treats as blocking.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use git2::{Oid, Repository, StatusOptions};
use intent_core::{Error, Result};

use crate::map_git_err;

/// Directory entries the walk visits before it gives up (an error, so the
/// purge is skipped rather than guessing).
const MAX_SCAN_ENTRIES: usize = 5_000_000;

/// Why a standalone checkout's folder cannot be purged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StandaloneWorkKind {
    /// Uncommitted, staged or untracked changes.
    Uncommitted,
    /// Commits no remote-tracking ref reaches.
    Unpushed,
    /// A stash entry.
    Stash,
    /// A linked worktree outside the folder.
    ExternalWorktree,
}

impl StandaloneWorkKind {
    /// Short human reason, used in logs and sweep summaries.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Uncommitted => "uncommitted changes",
            Self::Unpushed => "unpushed commits",
            Self::Stash => "a stash",
            Self::ExternalWorktree => "a linked worktree outside the folder",
        }
    }
}

/// The first blocking work found, and the repository that holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StandaloneWork {
    pub kind: StandaloneWorkKind,
    pub repo: PathBuf,
}

/// Evaluate every git repository under `scan_root` (see the module docs).
/// `merged_heads` are full commit ids of merged PR heads. `Ok(None)` when
/// nothing would be lost, including when `scan_root` does not exist.
///
/// # Errors
///
/// Returns `Error::Internal` when a directory cannot be read, the walk is
/// too large, or a repository cannot be evaluated.
pub fn standalone_work(
    scan_root: &Path,
    merged_heads: &[String],
) -> Result<Option<StandaloneWork>> {
    if matches!(scan_root.try_exists(), Ok(false)) {
        return Ok(None);
    }
    let root = std::fs::canonicalize(scan_root).map_err(|e| io_err(scan_root, &e))?;
    let mut stack = vec![root.clone()];
    let mut visited = 0usize;
    while let Some(dir) = stack.pop() {
        let mut subdirs = Vec::new();
        let (mut has_git, mut cache_tag) = (false, false);
        let (mut head_file, mut objects_dir, mut refs_dir) = (false, false, false);
        for entry in std::fs::read_dir(&dir).map_err(|e| io_err(&dir, &e))? {
            let entry = entry.map_err(|e| io_err(&dir, &e))?;
            visited += 1;
            if visited > MAX_SCAN_ENTRIES {
                return Err(Error::Internal(format!(
                    "{} holds more than {MAX_SCAN_ENTRIES} entries",
                    root.display()
                )));
            }
            // `DirEntry::file_type` never follows symlinks.
            let kind = entry.file_type().map_err(|e| io_err(&entry.path(), &e))?;
            match entry.file_name().to_str() {
                Some(".git") => {
                    has_git = true;
                    continue;
                }
                Some("node_modules") => continue,
                Some("CACHEDIR.TAG") => cache_tag |= kind.is_file(),
                Some("HEAD") => head_file |= kind.is_file(),
                Some("objects") => objects_dir |= kind.is_dir(),
                Some("refs") => refs_dir |= kind.is_dir(),
                _ => {}
            }
            if kind.is_dir() {
                subdirs.push(entry.path());
            }
        }
        if cache_tag {
            continue;
        }
        if has_git {
            if let Some(work) = evaluate_repo(&dir, &root, merged_heads, false)? {
                return Ok(Some(work));
            }
        } else if head_file && objects_dir && refs_dir {
            if let Some(work) = evaluate_repo(&dir, &root, merged_heads, true)? {
                return Ok(Some(work));
            }
            continue;
        }
        stack.extend(subdirs);
    }
    Ok(None)
}

fn io_err(path: &Path, e: &std::io::Error) -> Error {
    Error::Internal(format!("cannot read {}: {e}", path.display()))
}

/// Evaluate the repository at `dir` (its work tree, or the bare repository
/// itself when `bare`).
fn evaluate_repo(
    dir: &Path,
    root: &Path,
    merged_heads: &[String],
    bare: bool,
) -> Result<Option<StandaloneWork>> {
    let repo = if bare {
        Repository::open_bare(dir)
    } else {
        Repository::open(dir)
    }
    .map_err(|e| Error::Internal(format!("{}: {}", dir.display(), e.message())))?;
    let found = |kind| {
        Ok(Some(StandaloneWork {
            kind,
            repo: dir.to_path_buf(),
        }))
    };
    if !bare && !repo.is_bare() && has_uncommitted(&repo, dir)? {
        return found(StandaloneWorkKind::Uncommitted);
    }
    if repo.find_reference("refs/stash").is_ok() {
        return found(StandaloneWorkKind::Stash);
    }
    if has_unpushed(&repo, dir, merged_heads)? {
        return found(StandaloneWorkKind::Unpushed);
    }
    if has_external_worktree(&repo, root)? {
        return found(StandaloneWorkKind::ExternalWorktree);
    }
    let modules = repo.commondir().join("modules");
    if !bare
        && modules.is_dir()
        && std::fs::canonicalize(&modules).is_ok_and(|m| m.starts_with(root))
    {
        return evaluate_module_gitdirs(&modules, root, merged_heads);
    }
    Ok(None)
}

/// Submodule repositories under a `.git/modules` directory (a name such as
/// `packages/sub` is a directory of directories; a submodule's own
/// submodules sit in its `modules/`): stashes, unpushed commits and linked
/// worktrees outside `root` block, whether or not the submodule is checked
/// out.
fn evaluate_module_gitdirs(
    dir: &Path,
    root: &Path,
    merged_heads: &[String],
) -> Result<Option<StandaloneWork>> {
    for entry in std::fs::read_dir(dir).map_err(|e| io_err(dir, &e))? {
        let entry = entry.map_err(|e| io_err(dir, &e))?;
        if !entry
            .file_type()
            .map_err(|e| io_err(&entry.path(), &e))?
            .is_dir()
        {
            continue;
        }
        let path = entry.path();
        let gitdir = path.join("HEAD").is_file()
            && path.join("objects").is_dir()
            && path.join("refs").is_dir();
        if !gitdir {
            if let Some(work) = evaluate_module_gitdirs(&path, root, merged_heads)? {
                return Ok(Some(work));
            }
            continue;
        }
        let repo = Repository::open(&path)
            .map_err(|e| Error::Internal(format!("{}: {}", path.display(), e.message())))?;
        let kind = if repo.find_reference("refs/stash").is_ok() {
            Some(StandaloneWorkKind::Stash)
        } else if has_unpushed(&repo, &path, merged_heads)? {
            Some(StandaloneWorkKind::Unpushed)
        } else if has_external_worktree(&repo, root)? {
            Some(StandaloneWorkKind::ExternalWorktree)
        } else {
            None
        };
        if let Some(kind) = kind {
            return Ok(Some(StandaloneWork { kind, repo: path }));
        }
        let nested = path.join("modules");
        if nested.is_dir() {
            if let Some(work) = evaluate_module_gitdirs(&nested, root, merged_heads)? {
                return Ok(Some(work));
            }
        }
    }
    Ok(None)
}

/// Uncommitted, staged or untracked changes, ignoring submodule entries
/// whose change is only a moved gitlink (see [`gitlink_is_safe`]).
fn has_uncommitted(repo: &Repository, dir: &Path) -> Result<bool> {
    let submodules: HashSet<String> = repo
        .submodules()
        .map_err(map_git_err)?
        .iter()
        .filter_map(|s| s.path().to_str().map(str::to_string))
        .collect();
    let mut opts = StatusOptions::new();
    opts.include_untracked(true)
        .recurse_untracked_dirs(false)
        .include_ignored(false)
        .exclude_submodules(false);
    let statuses = repo.statuses(Some(&mut opts)).map_err(map_git_err)?;
    for entry in statuses.iter() {
        if entry.status().is_empty() || entry.status().is_ignored() {
            continue;
        }
        let path = entry
            .path()
            .map(|p| p.trim_end_matches('/').to_string())
            .map_err(|_| Error::Internal(format!("{}: non-UTF-8 path", dir.display())))?;
        if submodules.contains(&path) && gitlink_is_safe(repo, dir, &path)? {
            continue;
        }
        return Ok(true);
    }
    Ok(false)
}

/// A changed submodule entry loses nothing when the submodule is checked
/// out (the walk evaluates its own changes and commits) and any staged
/// gitlink commit is reached by one of the submodule's remote refs.
fn gitlink_is_safe(repo: &Repository, dir: &Path, path: &str) -> Result<bool> {
    let sub_dir = dir.join(path);
    if !sub_dir.join(".git").exists() {
        return Ok(false);
    }
    let Ok(sub) = Repository::open(&sub_dir) else {
        return Ok(false);
    };
    let index = repo.index().map_err(map_git_err)?;
    let staged = index.get_path(Path::new(path), 0).map(|e| e.id);
    let committed = repo
        .head()
        .ok()
        .and_then(|h| h.peel_to_tree().ok())
        .and_then(|t| t.get_path(Path::new(path)).ok())
        .map(|e| e.id());
    match staged {
        Some(oid) if Some(oid) != committed => {
            if sub.find_commit(oid).is_err() {
                return Ok(false);
            }
            Ok(count_unreached(&sub, &[oid], &[])? == 0)
        }
        _ => Ok(true),
    }
}

/// Commits reachable from `HEAD` or a local ref (anything outside
/// `refs/remotes/`; the stash is checked on its own) that no remote ref
/// reaches; a merged PR head covering `HEAD` lifts the commits `HEAD`
/// reaches. A branch that does not resolve to a commit is an error; a tag
/// or other ref on a non-commit object holds no commits and is skipped.
fn has_unpushed(repo: &Repository, dir: &Path, merged_heads: &[String]) -> Result<bool> {
    let head = repo
        .head()
        .ok()
        .and_then(|h| h.peel_to_commit().ok())
        .map(|c| c.id());
    let mut tips: Vec<Oid> = head.into_iter().collect();
    for reference in repo.references().map_err(map_git_err)? {
        let reference = reference.map_err(map_git_err)?;
        let name = reference.name().map_err(map_git_err)?;
        if name.starts_with("refs/remotes/") || name == "refs/stash" {
            continue;
        }
        match reference.peel_to_commit() {
            Ok(commit) => tips.push(commit.id()),
            Err(e) if name.starts_with("refs/heads/") => return Err(map_git_err(e)),
            Err(_) => {}
        }
    }
    if count_unreached(repo, &tips, &[])? == 0 {
        return Ok(false);
    }
    let Some(head) = head else {
        return Ok(true);
    };
    if !repo.is_bare() && !merged_heads.is_empty() && crate::head_reached_by(dir, merged_heads)? {
        return Ok(count_unreached(repo, &tips, &[head])? > 0);
    }
    Ok(true)
}

/// Commits reachable from `tips` but from neither `also_hide` nor any
/// `refs/remotes/*` ref, stopping at the first one.
fn count_unreached(repo: &Repository, tips: &[Oid], also_hide: &[Oid]) -> Result<usize> {
    if tips.is_empty() {
        return Ok(0);
    }
    let mut walk = repo.revwalk().map_err(map_git_err)?;
    for &tip in tips {
        walk.push(tip).map_err(map_git_err)?;
    }
    for reference in repo
        .references_glob("refs/remotes/*")
        .map_err(map_git_err)?
    {
        let reference = reference.map_err(map_git_err)?;
        // A dangling remote ref (an `origin/HEAD` whose branch is gone)
        // hides nothing; skipping it only over-counts.
        if let Ok(commit) = reference.peel_to_commit() {
            let _ = walk.hide(commit.id());
        }
    }
    for &oid in also_hide {
        walk.hide(oid).map_err(map_git_err)?;
    }
    match walk.next() {
        None => Ok(0),
        Some(oid) => oid.map(|_| 1).map_err(map_git_err),
    }
}

/// A linked worktree of `repo` whose (existing) folder is outside `root`.
fn has_external_worktree(repo: &Repository, root: &Path) -> Result<bool> {
    let names = repo.worktrees().map_err(map_git_err)?;
    for name in names.iter().flatten().flatten() {
        let Ok(worktree) = repo.find_worktree(name) else {
            continue;
        };
        let Ok(path) = std::fs::canonicalize(worktree.path()) else {
            continue;
        };
        if !path.starts_with(root) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::{standalone_work, StandaloneWork, StandaloneWorkKind};
    use crate::testutil::{allow_file_submodules, init_repo, TempDir};
    use std::path::{Path, PathBuf};

    #[track_caller]
    fn git(dir: &Path, args: &[&str]) -> String {
        let out = std::process::Command::new("git")
            .arg("-C")
            .arg(dir)
            .args(["-c", "protocol.file.allow=always"])
            .args(args)
            .env("GIT_AUTHOR_NAME", "t")
            .env("GIT_AUTHOR_EMAIL", "t@t")
            .env("GIT_COMMITTER_NAME", "t")
            .env("GIT_COMMITTER_EMAIL", "t@t")
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .output()
            .expect("run git");
        assert!(
            out.status.success(),
            "git {args:?} in {}: {}",
            dir.display(),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    fn commit(dir: &Path, name: &str) -> String {
        std::fs::write(dir.join(name), format!("{name}\n")).unwrap();
        git(dir, &["add", name]);
        git(dir, &["commit", "-q", "-m", name]);
        git(dir, &["rev-parse", "HEAD"])
    }

    /// A bare `origin` with one commit on `main`, and `<ws>/repo` cloned
    /// from it: the daemon-owned standalone layout, every commit pushed.
    struct Layout {
        tmp: TempDir,
        ws: PathBuf,
        repo: PathBuf,
    }

    fn layout(tag: &str) -> Layout {
        let tmp = init_repo(tag);
        let seed = tmp.path().join("seed");
        std::fs::create_dir_all(&seed).unwrap();
        git(&seed, &["init", "-q", "-b", "main"]);
        commit(&seed, "README.md");
        git(tmp.path(), &["clone", "-q", "--bare", "seed", "origin.git"]);
        let ws = tmp.path().join("ws-1");
        std::fs::create_dir_all(&ws).unwrap();
        git(&ws, &["clone", "-q", "../origin.git", "repo"]);
        let repo = ws.join("repo");
        Layout { tmp, ws, repo }
    }

    fn scan(l: &Layout) -> Option<StandaloneWork> {
        standalone_work(&l.ws, &[]).expect("scan")
    }

    fn kind(l: &Layout) -> Option<StandaloneWorkKind> {
        scan(l).map(|w| w.kind)
    }

    #[test]
    fn a_clean_pushed_clone_and_a_missing_folder_hold_nothing() {
        let l = layout("standalone-clean");
        std::fs::create_dir_all(l.ws.join(".workspace")).unwrap();
        assert_eq!(scan(&l), None);
        assert_eq!(
            standalone_work(&l.tmp.path().join("gone"), &[]).unwrap(),
            None
        );
    }

    #[test]
    fn untracked_modified_and_staged_files_block() {
        let l = layout("standalone-dirty");
        std::fs::write(l.repo.join("draft.txt"), "x").unwrap();
        assert_eq!(kind(&l), Some(StandaloneWorkKind::Uncommitted));
        std::fs::remove_file(l.repo.join("draft.txt")).unwrap();
        std::fs::write(l.repo.join("README.md"), "edited").unwrap();
        assert_eq!(kind(&l), Some(StandaloneWorkKind::Uncommitted));
        git(&l.repo, &["add", "README.md"]);
        assert_eq!(kind(&l), Some(StandaloneWorkKind::Uncommitted));
    }

    #[test]
    fn an_unpushed_commit_on_a_branch_other_than_head_blocks() {
        let l = layout("standalone-branch");
        git(&l.repo, &["checkout", "-q", "-b", "side"]);
        commit(&l.repo, "side.txt");
        git(&l.repo, &["checkout", "-q", "main"]);
        assert_eq!(kind(&l), Some(StandaloneWorkKind::Unpushed));
    }

    #[test]
    fn an_unpushed_commit_on_a_detached_head_blocks() {
        let l = layout("standalone-detached");
        git(&l.repo, &["checkout", "-q", "--detach"]);
        commit(&l.repo, "detached.txt");
        assert_eq!(kind(&l), Some(StandaloneWorkKind::Unpushed));
    }

    #[test]
    fn a_repository_without_remotes_blocks() {
        let l = layout("standalone-no-remote");
        git(&l.repo, &["remote", "remove", "origin"]);
        assert_eq!(kind(&l), Some(StandaloneWorkKind::Unpushed));
    }

    #[test]
    fn a_stash_blocks() {
        let l = layout("standalone-stash");
        std::fs::write(l.repo.join("README.md"), "wip").unwrap();
        git(&l.repo, &["stash", "-q"]);
        assert_eq!(kind(&l), Some(StandaloneWorkKind::Stash));
    }

    /// Nested repositories under an ignored `.dev/` are found: a clone with
    /// an unpushed commit, and a linked worktree with uncommitted changes.
    #[test]
    fn nested_repositories_in_ignored_folders_are_evaluated() {
        let l = layout("standalone-nested");
        std::fs::write(l.repo.join(".git/info/exclude"), ".dev/\n").unwrap();
        let dev = l.repo.join(".dev");
        std::fs::create_dir_all(&dev).unwrap();
        git(&dev, &["clone", "-q", "../../../origin.git", "clone"]);
        assert_eq!(scan(&l), None, "a pushed nested clone loses nothing");
        commit(&dev.join("clone"), "nested.txt");
        assert_eq!(
            scan(&l),
            Some(StandaloneWork {
                kind: StandaloneWorkKind::Unpushed,
                repo: std::fs::canonicalize(dev.join("clone")).unwrap(),
            })
        );
        std::fs::remove_dir_all(dev.join("clone")).unwrap();

        git(&l.repo, &["worktree", "add", "-q", ".dev/wt", "-b", "wt"]);
        assert_eq!(scan(&l), None, "the worktree branch is at origin/main");
        std::fs::write(dev.join("wt/README.md"), "edited in the worktree").unwrap();
        assert_eq!(kind(&l), Some(StandaloneWorkKind::Uncommitted));
    }

    #[test]
    fn a_linked_worktree_outside_the_folder_blocks() {
        let l = layout("standalone-external");
        let outside = l.tmp.path().join("outside");
        git(
            &l.repo,
            &[
                "worktree",
                "add",
                "-q",
                outside.to_str().unwrap(),
                "origin/main",
            ],
        );
        assert_eq!(kind(&l), Some(StandaloneWorkKind::ExternalWorktree));
    }

    /// A merged PR head covering `HEAD` lifts the commits `HEAD` reaches,
    /// but not a commit only another branch reaches.
    #[test]
    fn a_merged_pr_covering_head_lifts_only_head_commits() {
        let l = layout("standalone-merged");
        git(&l.repo, &["checkout", "-q", "-b", "feature"]);
        let head = commit(&l.repo, "feature.txt");
        assert_eq!(kind(&l), Some(StandaloneWorkKind::Unpushed));
        assert_eq!(
            standalone_work(&l.ws, std::slice::from_ref(&head)).unwrap(),
            None
        );
        git(&l.repo, &["checkout", "-q", "-b", "other", "main"]);
        commit(&l.repo, "other.txt");
        git(&l.repo, &["checkout", "-q", "feature"]);
        assert_eq!(
            standalone_work(&l.ws, &[head]).unwrap().map(|w| w.kind),
            Some(StandaloneWorkKind::Unpushed)
        );
    }

    /// A submodule whose gitlink moved to a pushed commit loses nothing; an
    /// unpushed commit or a dirty file inside the submodule blocks.
    #[test]
    fn a_gitlink_moved_to_a_pushed_commit_does_not_block() {
        allow_file_submodules();
        let l = layout("standalone-gitlink");
        let sub_src = l.tmp.path().join("sub-src");
        std::fs::create_dir_all(&sub_src).unwrap();
        git(&sub_src, &["init", "-q", "-b", "main"]);
        commit(&sub_src, "a.txt");
        git(
            l.tmp.path(),
            &["clone", "-q", "--bare", "sub-src", "sub.git"],
        );
        let sub_url = l.tmp.path().join("sub.git");
        git(
            &l.repo,
            &["submodule", "add", "-q", sub_url.to_str().unwrap(), "sub"],
        );
        git(&l.repo, &["commit", "-q", "-m", "add sub"]);
        git(&l.repo, &["push", "-q", "origin", "HEAD"]);
        assert_eq!(scan(&l), None, "clean submodule");

        commit(&sub_src, "b.txt");
        git(&sub_src, &["push", "-q", sub_url.to_str().unwrap(), "main"]);
        let sub = l.repo.join("sub");
        git(&sub, &["fetch", "-q", "origin"]);
        git(&sub, &["merge", "-q", "--ff-only", "origin/main"]);
        assert_eq!(scan(&l), None, "gitlink moved to a pushed commit");
        git(&l.repo, &["add", "sub"]);
        assert_eq!(scan(&l), None, "staged gitlink names a pushed commit");

        commit(&sub, "local.txt");
        assert_eq!(kind(&l), Some(StandaloneWorkKind::Unpushed));
        git(&sub, &["reset", "-q", "--hard", "origin/main"]);
        std::fs::write(sub.join("a.txt"), "dirty").unwrap();
        assert_eq!(kind(&l), Some(StandaloneWorkKind::Uncommitted));
    }

    /// Every local ref counts, not only branches: a remote agent's
    /// `refs/intent/...` work ref, or a tag, on a commit no remote has.
    #[test]
    fn a_commit_only_a_non_branch_ref_reaches_blocks() {
        let l = layout("standalone-intent-ref");
        for name in ["refs/intent/ws/ws-1/agents/a/head", "refs/tags/v-local"] {
            let work = commit(&l.repo, "agent.txt");
            git(&l.repo, &["update-ref", name, &work]);
            git(&l.repo, &["reset", "-q", "--hard", "origin/main"]);
            assert_eq!(kind(&l), Some(StandaloneWorkKind::Unpushed), "{name}");
            git(&l.repo, &["update-ref", "-d", name]);
            assert_eq!(scan(&l), None, "{name} removed");
        }
    }

    /// A deinitialized submodule keeps its repository in `.git/modules`:
    /// its unpushed commits block.
    #[test]
    fn a_deinitialized_submodule_with_unpushed_commits_blocks() {
        allow_file_submodules();
        let l = layout("standalone-deinit");
        let sub_src = l.tmp.path().join("sub-src");
        std::fs::create_dir_all(&sub_src).unwrap();
        git(&sub_src, &["init", "-q", "-b", "main"]);
        commit(&sub_src, "a.txt");
        git(
            l.tmp.path(),
            &["clone", "-q", "--bare", "sub-src", "sub.git"],
        );
        let sub_url = l.tmp.path().join("sub.git");
        git(
            &l.repo,
            &[
                "submodule",
                "add",
                "-q",
                sub_url.to_str().unwrap(),
                "libs/sub",
            ],
        );
        git(&l.repo, &["commit", "-q", "-m", "add sub"]);
        git(&l.repo, &["push", "-q", "origin", "HEAD"]);
        git(&l.repo, &["submodule", "deinit", "-q", "-f", "libs/sub"]);
        assert_eq!(scan(&l), None, "a pushed deinitialized submodule");
        git(
            &l.repo,
            &["submodule", "update", "-q", "--init", "libs/sub"],
        );
        let sub = l.repo.join("libs/sub");
        git(&sub, &["checkout", "-q", "main"]);
        commit(&sub, "local.txt");
        git(&l.repo, &["submodule", "deinit", "-q", "-f", "libs/sub"]);
        assert!(!sub.join(".git").exists(), "the checkout is gone");
        assert_eq!(kind(&l), Some(StandaloneWorkKind::Unpushed));
    }

    /// A submodule repository under `.git/modules` that backs a linked
    /// worktree outside the folder blocks, checked out or not.
    #[test]
    fn a_module_repository_with_an_external_worktree_blocks() {
        allow_file_submodules();
        let l = layout("standalone-module-worktree");
        let sub_src = l.tmp.path().join("sub-src");
        std::fs::create_dir_all(&sub_src).unwrap();
        git(&sub_src, &["init", "-q", "-b", "main"]);
        commit(&sub_src, "a.txt");
        git(
            l.tmp.path(),
            &["clone", "-q", "--bare", "sub-src", "sub.git"],
        );
        let sub_url = l.tmp.path().join("sub.git");
        git(
            &l.repo,
            &["submodule", "add", "-q", sub_url.to_str().unwrap(), "sub"],
        );
        git(&l.repo, &["commit", "-q", "-m", "add sub"]);
        git(&l.repo, &["push", "-q", "origin", "HEAD"]);
        let outside = l.tmp.path().join("sub-outside");
        git(
            &l.repo.join("sub"),
            &[
                "worktree",
                "add",
                "-q",
                "--detach",
                outside.to_str().unwrap(),
            ],
        );
        git(&l.repo, &["submodule", "deinit", "-q", "-f", "sub"]);
        assert_eq!(kind(&l), Some(StandaloneWorkKind::ExternalWorktree));
    }

    #[test]
    fn a_bare_repository_with_unreached_branches_blocks() {
        let l = layout("standalone-bare");
        git(&l.repo, &["clone", "-q", "--bare", ".", "local-remote.git"]);
        std::fs::write(l.repo.join(".git/info/exclude"), "local-remote.git/\n").unwrap();
        assert_eq!(kind(&l), Some(StandaloneWorkKind::Unpushed));
    }

    /// Cache directories (`CACHEDIR.TAG`) and `node_modules` are build output
    /// and are not walked.
    #[test]
    fn cache_directories_are_not_walked() {
        let l = layout("standalone-cache");
        std::fs::write(l.repo.join(".git/info/exclude"), "target/\nnode_modules/\n").unwrap();
        for dir in ["target", "node_modules"] {
            let nested = l.repo.join(dir).join("dep");
            std::fs::create_dir_all(&nested).unwrap();
            git(&nested, &["init", "-q"]);
            std::fs::write(nested.join("x"), "x").unwrap();
        }
        std::fs::write(
            l.repo.join("target/CACHEDIR.TAG"),
            "Signature: 8a477f597d28d172789f06886806bc55\n",
        )
        .unwrap();
        assert_eq!(scan(&l), None);
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_are_not_followed() {
        let l = layout("standalone-symlink");
        let outside = l.tmp.path().join("elsewhere");
        std::fs::create_dir_all(&outside).unwrap();
        git(&outside, &["init", "-q"]);
        std::fs::write(outside.join("x"), "x").unwrap();
        std::os::unix::fs::symlink(&outside, l.ws.join("link")).unwrap();
        assert_eq!(scan(&l), None);
    }
}
