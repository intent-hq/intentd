//! Unpublished-submodule detection for workspace transfer (monorepo#4219).
//!
//! A transfer bundle carries the superproject's history, and a gitlink in
//! that history is only a SHA — the destination hydrates each submodule from
//! its origin. When the checked-out submodule commit exists only on the
//! source machine (never pushed), hydration fails and the work is lost. This
//! module finds those commits so the plan can warn about them and the export
//! can bundle them.
//!
//! "Published" is decided locally, without network: a commit is published when
//! it is reachable from (or equal to) any `refs/remotes/**` tip of that
//! submodule repo. A stale remote-tracking ref only causes an unnecessary but
//! harmless bundle.
//!
//! A nested unpublished submodule can only be hydrated once its containing
//! submodule is checked out, so every ancestor of an unpublished finding is
//! reported too — flagged [`UnpublishedSubmodule::published`] when its own
//! commit is reachable from a remote — and bundled alongside.

use std::path::{Component, Path, PathBuf};

use git2::{Oid, Repository};
use intent_core::{Error, Result};

/// One tracked submodule whose checkout HEAD is not reachable from any of its
/// remote-tracking refs — or a published ancestor carried so that such a
/// nested submodule can be hydrated (`published: true`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnpublishedSubmodule {
    /// The submodule's registered name (`submodule.<name>.*`) in ITS
    /// superproject — raw, not composed, so hydration can address the
    /// config key directly. Falls back to the path when unnamed.
    pub name: String,
    /// Path relative to the workspace worktree root, forward slashes; a
    /// nested submodule composes its parents' paths (`sub/inner`).
    pub path: String,
    /// The submodule checkout's HEAD commit (full hex).
    pub commit_sha: String,
    /// HEAD branch shorthand when HEAD is attached.
    pub branch: Option<String>,
    /// `remote.origin.url` of the submodule checkout, when configured.
    pub origin_url: Option<String>,
    /// The submodule checkout on disk (its work tree).
    pub repo_dir: PathBuf,
    /// `true` when this commit IS reachable from a remote and the entry is
    /// only present because a nested submodule below it is unpublished: the
    /// destination needs this checkout to place the nested one.
    pub published: bool,
}

/// Find every initialized submodule (nested ones included, up to
/// [`intent_git::submodule::MAX_SUBMODULE_NESTING`]) of the repository at
/// `repo_path` whose HEAD commit is unpublished, plus every ancestor
/// submodule of such a finding (flagged `published` when its own commit is
/// reachable from a remote). Uninitialized submodules and unborn checkouts
/// are skipped — there is nothing local to lose. Results are sorted by
/// `path`, so a parent always precedes its nested children.
///
/// # Errors
///
/// `Error::Internal` when `repo_path` is not an openable repository.
pub(crate) fn find_unpublished_submodules(repo_path: &Path) -> Result<Vec<UnpublishedSubmodule>> {
    let repo = Repository::open(repo_path)
        .map_err(|e| Error::Internal(format!("open repository for submodule scan: {e}")))?;
    let mut out = Vec::new();
    collect(&repo, repo_path, "", 0, &mut out);
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

fn collect(
    repo: &Repository,
    workdir: &Path,
    prefix: &str,
    depth: u32,
    out: &mut Vec<UnpublishedSubmodule>,
) {
    let Ok(submodules) = repo.submodules() else {
        return;
    };
    for sm in submodules {
        let rel = sm.path().to_string_lossy().replace('\\', "/");
        let Some(rel_path) = safe_rel_path(&rel) else {
            continue;
        };
        // `Submodule::open` fails for a registered-but-uninitialized
        // submodule (no checkout on disk): nothing local to lose, skip.
        let Ok(sub_repo) = sm.open() else {
            continue;
        };
        let sub_dir = workdir.join(rel_path);
        let full_path = if prefix.is_empty() {
            rel.clone()
        } else {
            format!("{prefix}/{rel}")
        };
        let name = sm
            .name()
            .ok()
            .filter(|n| !n.is_empty())
            .map_or_else(|| rel.clone(), str::to_string);

        let Some(found) = inspect_checkout(&sub_repo, &sub_dir, name, &full_path) else {
            continue;
        };
        let before = out.len();
        if depth + 1 < intent_git::submodule::MAX_SUBMODULE_NESTING {
            collect(&sub_repo, &sub_dir, &full_path, depth + 1, out);
        }
        // Unpublished itself, or a published ancestor of a nested finding.
        if !found.published || out.len() > before {
            out.push(found);
        }
    }
}

/// Inspect one submodule checkout; `None` only for an unborn HEAD.
fn inspect_checkout(
    sub_repo: &Repository,
    sub_dir: &Path,
    name: String,
    full_path: &str,
) -> Option<UnpublishedSubmodule> {
    let head = sub_repo.head().ok()?;
    let head_oid = head.target()?;
    let published = is_reachable_from_remote(sub_repo, head_oid);
    let branch = if head.is_branch() {
        head.shorthand().ok().map(str::to_string)
    } else {
        None
    };
    let origin_url = sub_repo
        .find_remote("origin")
        .ok()
        .and_then(|r| r.url().ok().map(str::to_string));
    Some(UnpublishedSubmodule {
        name,
        path: full_path.to_string(),
        commit_sha: head_oid.to_string(),
        branch,
        origin_url,
        repo_dir: sub_dir.to_path_buf(),
        published,
    })
}

/// Whether `oid` equals or is an ancestor of any `refs/remotes/**` tip.
/// No remote refs at all ⇒ unreachable (unpublished).
fn is_reachable_from_remote(repo: &Repository, oid: Oid) -> bool {
    let Ok(refs) = repo.references_glob("refs/remotes/*") else {
        return false;
    };
    for reference in refs.flatten() {
        let Some(tip) = reference
            .resolve()
            .ok()
            .and_then(|r| r.peel_to_commit().ok())
            .map(|c| c.id())
        else {
            continue;
        };
        if tip == oid || repo.graph_descendant_of(tip, oid).unwrap_or(false) {
            return true;
        }
    }
    false
}

/// A submodule path that is safe to join under its superproject: relative
/// and free of `..` / root components.
fn safe_rel_path(rel: &str) -> Option<PathBuf> {
    let p = Path::new(rel);
    if rel.is_empty()
        || !p
            .components()
            .all(|c| matches!(c, Component::Normal(_) | Component::CurDir))
    {
        return None;
    }
    Some(p.to_path_buf())
}

/// Estimated on-disk bytes of every object reachable from the submodule's
/// commit (`git rev-list --disk-usage --objects <sha>` in the submodule
/// repo) — the size its self-contained bundle would add to the archive.
/// Degrades to 0 when git is unavailable or the commit cannot be walked.
pub(crate) fn estimate_submodule_bundle_bytes(sub: &UnpublishedSubmodule) -> u64 {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(&sub.repo_dir)
        .args(["rev-list", "--disk-usage", "--objects"])
        .arg(&sub.commit_sha)
        .output();
    match out {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout)
            .trim()
            .parse::<u64>()
            .unwrap_or(0),
        _ => 0,
    }
}

#[cfg(test)]
pub(crate) mod test_fixture {
    //! Temp superproject + submodule fixture shared by the plan tests.

    use std::path::{Path, PathBuf};

    /// Run `git` in `dir` with a fixed identity; panics on failure.
    pub(crate) fn git(dir: &Path, args: &[&str]) -> String {
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
            "git {args:?} in {} failed: {}",
            dir.display(),
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    }

    /// A repository with one seed commit on `main`.
    pub(crate) fn init_repo(dir: &Path) {
        std::fs::create_dir_all(dir).expect("mkdir");
        git(dir, &["init", "-q", "-b", "main"]);
        std::fs::write(dir.join("README.md"), "hello\n").expect("write");
        git(dir, &["add", "."]);
        git(dir, &["commit", "-q", "-m", "seed"]);
    }

    /// Superproject `super/` tracking submodule `sub` whose origin is the
    /// bare `origin.git` (seeded from `sub-src/`). Returns
    /// `(superproject, bare_origin)`; the submodule checkout at
    /// `super/sub` has `origin/main` pointing at the seed commit.
    pub(crate) fn superproject_with_submodule(root: &Path) -> (PathBuf, PathBuf) {
        let sub_src = root.join("sub-src");
        init_repo(&sub_src);
        let origin = root.join("origin.git");
        git(root, &["clone", "-q", "--bare", "sub-src", "origin.git"]);
        let sup = root.join("super");
        init_repo(&sup);
        git(
            sup.as_path(),
            &["submodule", "add", "-q", origin.to_str().unwrap(), "sub"],
        );
        git(sup.as_path(), &["commit", "-q", "-m", "add submodule"]);
        (sup, origin)
    }

    /// Commit a new file inside the checkout at `dir`; returns the SHA.
    pub(crate) fn local_commit(dir: &Path, file: &str) -> String {
        std::fs::write(dir.join(file), format!("{file}\n")).expect("write");
        git(dir, &["add", file]);
        git(dir, &["commit", "-q", "-m", &format!("local {file}")]);
        git(dir, &["rev-parse", "HEAD"])
    }

    /// The nested-under-published-parent scenario (monorepo#4219 follow-up).
    pub(crate) struct NestedFixture {
        pub sup: PathBuf,
        pub sub_origin: PathBuf,
        pub inner_origin: PathBuf,
        /// `sub`'s HEAD: records `inner` at `inner_sha`, pushed to its origin.
        pub sub_sha: String,
        /// `sub/inner`'s HEAD on `feat/x`: never pushed.
        pub inner_sha: String,
    }

    /// Superproject `super/` → submodule `sub` (PUBLISHED: its `main`,
    /// bumping the nested `inner` gitlink to a local-only commit, is pushed
    /// to `origin.git`) → nested `sub/inner` on `feat/x` at an UNPUBLISHED
    /// commit (`deep.txt`). The superproject's gitlink for `sub` is stale
    /// (`M sub`), as a WIP snapshot would carry it.
    pub(crate) fn nested_unpublished_under_published_parent(root: &Path) -> NestedFixture {
        let inner_src = root.join("inner-src");
        init_repo(&inner_src);
        git(root, &["clone", "-q", "--bare", "inner-src", "inner.git"]);
        let inner_origin = root.join("inner.git");
        let (sup, sub_origin) = superproject_with_submodule(root);
        let sub = sup.join("sub");
        git(&sub, &["checkout", "-q", "main"]);
        git(
            &sub,
            &[
                "submodule",
                "add",
                "-q",
                inner_origin.to_str().unwrap(),
                "inner",
            ],
        );
        git(&sub, &["commit", "-q", "-m", "add inner"]);
        let inner = sub.join("inner");
        git(&inner, &["checkout", "-q", "-b", "feat/x"]);
        let inner_sha = local_commit(&inner, "deep.txt");
        git(&sub, &["add", "inner"]);
        git(&sub, &["commit", "-q", "-m", "bump inner"]);
        git(&sub, &["push", "-q", "origin", "main"]);
        let sub_sha = git(&sub, &["rev-parse", "HEAD"]);
        NestedFixture {
            sup,
            sub_origin,
            inner_origin,
            sub_sha,
            inner_sha,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_fixture::{
        git, init_repo, local_commit, nested_unpublished_under_published_parent,
        superproject_with_submodule, NestedFixture,
    };
    use super::*;

    /// A commit made inside the submodule checkout and never pushed is
    /// detected with its name, path, sha, branch and origin URL; pushing it
    /// to the bare origin (and thus updating `origin/main`) publishes it.
    #[test]
    fn detects_local_only_submodule_commit_until_pushed() {
        let tmp = tempfile::tempdir().unwrap();
        let (sup, origin) = superproject_with_submodule(tmp.path());
        let sub = sup.join("sub");
        assert!(find_unpublished_submodules(&sup).unwrap().is_empty());

        git(&sub, &["checkout", "-q", "main"]);
        let sha = local_commit(&sub, "wip.txt");
        let found = find_unpublished_submodules(&sup).unwrap();
        assert_eq!(found.len(), 1, "{found:?}");
        let f = &found[0];
        assert_eq!(f.name, "sub");
        assert_eq!(f.path, "sub");
        assert_eq!(f.commit_sha, sha);
        assert_eq!(f.branch.as_deref(), Some("main"));
        assert!(!f.published);
        assert_eq!(
            f.origin_url.as_deref().map(|u| u.trim_end_matches('/')),
            Some(origin.to_str().unwrap())
        );
        assert_eq!(f.repo_dir, sub);
        assert!(estimate_submodule_bundle_bytes(f) > 0);

        git(&sub, &["push", "-q", "origin", "main"]);
        assert!(find_unpublished_submodules(&sup).unwrap().is_empty());
    }

    /// A detached HEAD at an unpublished commit is reported with no branch;
    /// a detached HEAD at a published ancestor is not reported.
    #[test]
    fn detached_head_reports_no_branch() {
        let tmp = tempfile::tempdir().unwrap();
        let (sup, _origin) = superproject_with_submodule(tmp.path());
        let sub = sup.join("sub");
        git(&sub, &["checkout", "-q", "--detach"]);
        assert!(find_unpublished_submodules(&sup).unwrap().is_empty());
        let sha = local_commit(&sub, "detached.txt");
        let found = find_unpublished_submodules(&sup).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].commit_sha, sha);
        assert_eq!(found[0].branch, None);
    }

    /// A registered but uninitialized submodule (fresh non-recursive clone
    /// of the superproject) is skipped without error.
    #[test]
    fn skips_uninitialized_submodule() {
        let tmp = tempfile::tempdir().unwrap();
        let (sup, _origin) = superproject_with_submodule(tmp.path());
        git(tmp.path(), &["clone", "-q", sup.to_str().unwrap(), "clone"]);
        let clone = tmp.path().join("clone");
        assert!(clone.join("sub").is_dir());
        assert!(!clone.join("sub/.git").exists());
        assert!(find_unpublished_submodules(&clone).unwrap().is_empty());
    }

    /// A nested submodule (`sub/inner`) with a local-only commit is found
    /// with the composed path and its own raw name; its published parent
    /// `sub` is carried ahead of it (`published: true`) and flips to an
    /// ordinary unpublished finding once it records the child locally.
    #[test]
    fn detects_nested_submodule_with_composed_path() {
        let tmp = tempfile::tempdir().unwrap();
        let inner_src = tmp.path().join("inner-src");
        init_repo(&inner_src);
        git(
            tmp.path(),
            &["clone", "-q", "--bare", "inner-src", "inner.git"],
        );
        let (sup, _origin) = superproject_with_submodule(tmp.path());
        let sub = sup.join("sub");
        git(&sub, &["checkout", "-q", "main"]);
        let inner_origin = tmp.path().join("inner.git");
        git(
            &sub,
            &[
                "submodule",
                "add",
                "-q",
                inner_origin.to_str().unwrap(),
                "inner",
            ],
        );
        git(&sub, &["commit", "-q", "-m", "add inner"]);
        git(&sub, &["push", "-q", "origin", "main"]);
        assert!(find_unpublished_submodules(&sup).unwrap().is_empty());

        let inner = sub.join("inner");
        git(&inner, &["checkout", "-q", "-b", "feat/x"]);
        let sha = local_commit(&inner, "deep.txt");
        let found = find_unpublished_submodules(&sup).unwrap();
        assert_eq!(found.len(), 2, "{found:?}");
        assert_eq!(found[0].path, "sub");
        assert!(found[0].published, "published parent carried");
        assert_eq!(found[0].commit_sha, git(&sub, &["rev-parse", "HEAD"]));
        assert_eq!(found[1].name, "inner");
        assert_eq!(found[1].path, "sub/inner");
        assert_eq!(found[1].commit_sha, sha);
        assert_eq!(found[1].branch.as_deref(), Some("feat/x"));
        assert_eq!(found[1].repo_dir, inner);
        assert!(!found[1].published);

        git(&sub, &["add", "inner"]);
        git(&sub, &["commit", "-q", "-m", "bump inner"]);
        let found = find_unpublished_submodules(&sup).unwrap();
        assert_eq!(found.len(), 2, "{found:?}");
        assert_eq!(found[0].path, "sub");
        assert!(!found[0].published, "parent is now unpublished itself");
        assert!(!found[1].published);
    }

    /// Published ancestors are carried only when a nested descendant is
    /// unpublished: with the parent published and the child pushed, nothing
    /// is reported; a published ancestor keeps its own branch/origin/HEAD.
    #[test]
    fn carries_published_parent_chain_only_for_nested_findings() {
        let tmp = tempfile::tempdir().unwrap();
        let NestedFixture {
            sup,
            sub_origin,
            inner_origin,
            sub_sha,
            inner_sha,
        } = nested_unpublished_under_published_parent(tmp.path());
        let found = find_unpublished_submodules(&sup).unwrap();
        assert_eq!(found.len(), 2, "{found:?}");
        let parent = &found[0];
        assert_eq!(parent.name, "sub");
        assert_eq!(parent.path, "sub");
        assert_eq!(parent.commit_sha, sub_sha);
        assert_eq!(parent.branch.as_deref(), Some("main"));
        assert_eq!(
            parent
                .origin_url
                .as_deref()
                .map(|u| u.trim_end_matches('/')),
            Some(sub_origin.to_str().unwrap())
        );
        assert!(parent.published);
        assert!(estimate_submodule_bundle_bytes(parent) > 0);
        let child = &found[1];
        assert_eq!(child.path, "sub/inner");
        assert_eq!(child.commit_sha, inner_sha);
        assert_eq!(
            child.origin_url.as_deref().map(|u| u.trim_end_matches('/')),
            Some(inner_origin.to_str().unwrap())
        );
        assert!(!child.published);

        git(&sup.join("sub/inner"), &["push", "-q", "origin", "feat/x"]);
        assert!(find_unpublished_submodules(&sup).unwrap().is_empty());
    }

    /// A submodule with no remote-tracking refs at all is unpublished.
    #[test]
    fn no_remote_refs_means_unpublished() {
        let tmp = tempfile::tempdir().unwrap();
        let (sup, _origin) = superproject_with_submodule(tmp.path());
        let sub = sup.join("sub");
        git(&sub, &["update-ref", "-d", "refs/remotes/origin/main"]);
        git(&sub, &["update-ref", "-d", "refs/remotes/origin/HEAD"]);
        let found = find_unpublished_submodules(&sup).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].path, "sub");
    }

    /// A non-repository path is an error; detection never writes anything.
    #[test]
    fn non_repo_errors_and_scan_is_read_only() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(find_unpublished_submodules(tmp.path()).is_err());

        let (sup, _origin) = superproject_with_submodule(tmp.path());
        let sub = sup.join("sub");
        local_commit(&sub, "ro.txt");
        let before = git(&sup, &["status", "--porcelain"]);
        let sub_before = git(&sub, &["status", "--porcelain"]);
        let _ = find_unpublished_submodules(&sup).unwrap();
        assert_eq!(git(&sup, &["status", "--porcelain"]), before);
        assert_eq!(git(&sub, &["status", "--porcelain"]), sub_before);
    }
}
