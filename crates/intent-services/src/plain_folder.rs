//! Plain-folder workspace create checks (PROTOCOL §5.1 "Plain-folder
//! workspaces").
//!
//! A `workspace.create { plainFolder: true }` works directly in an ordinary
//! non-git folder. Everything here runs before any side effect: the request
//! shape check ([`validate_create_shape`]) is pure, and the path check
//! ([`resolve_target`]) only reads the filesystem. The single side effect —
//! creating a new folder — is [`PlainFolderTarget::materialize`], called by
//! the create once every other validation has passed.

use std::path::{Component, Path, PathBuf};

use intent_core::{
    ContextLinkKind, Error, PlainFolderPathReason, PlainFolderPathRejection, Result,
    WorkspaceCreate,
};

/// `true` when the request selects plain-folder mode.
pub(crate) fn requested(input: &WorkspaceCreate) -> bool {
    input.plain_folder == Some(true)
}

fn nonblank(value: Option<&str>) -> bool {
    value.is_some_and(|v| !v.trim().is_empty())
}

fn invalid(message: impl Into<String>) -> Error {
    Error::PlainFolderInvalidParams(message.into())
}

/// Request-shape rejections for a plain-folder create: git-only options, a
/// pr-kind context link, or a missing `repositoryPath`. A request without
/// `plainFolder: true` always passes.
///
/// # Errors
///
/// [`Error::PlainFolderInvalidParams`] naming the offending option.
pub(crate) fn validate_create_shape(input: &WorkspaceCreate) -> Result<()> {
    if !requested(input) {
        return Ok(());
    }
    let git_only = [
        ("githubUrl", nonblank(input.github_url.as_deref())),
        ("repositoryCheckout", input.repository_checkout.is_some()),
        ("clonePath", nonblank(input.clone_path.as_deref())),
        ("isNewRepo", input.is_new_repo == Some(true)),
        ("baseRef", nonblank(input.base_ref.as_deref())),
        ("branch", nonblank(input.branch.as_deref())),
        ("worktreePath", nonblank(input.worktree_path.as_deref())),
        ("isRemote", input.is_remote == Some(true)),
    ];
    if let Some((name, _)) = git_only.iter().find(|(_, set)| *set) {
        return Err(invalid(format!(
            "{name} cannot be combined with plainFolder: plain-folder workspaces have no git repository"
        )));
    }
    if input
        .context_links
        .as_deref()
        .is_some_and(|links| links.iter().any(|l| l.kind == ContextLinkKind::Pr))
    {
        return Err(invalid(
            "contextLinks: a pull-request link cannot be combined with plainFolder",
        ));
    }
    if !nonblank(input.repository_path.as_deref()) {
        return Err(invalid(
            "repositoryPath is required when plainFolder is true",
        ));
    }
    Ok(())
}

/// The folder a plain-folder create works in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PlainFolderTarget {
    /// An existing non-git folder: only the row is created.
    Existing(PathBuf),
    /// A folder that does not exist yet; its parent does.
    New(PathBuf),
}

impl PlainFolderTarget {
    /// The requested folder path.
    pub(crate) fn path(&self) -> &Path {
        match self {
            Self::Existing(path) | Self::New(path) => path,
        }
    }

    /// Create the folder for [`PlainFolderTarget::New`] (that one directory,
    /// no `.git`); a no-op for an existing folder. Returns `true` when this
    /// call created the directory, so a failed create can remove it again.
    ///
    /// # Errors
    ///
    /// `Error::Internal` when the directory cannot be created.
    pub(crate) fn materialize(&self) -> Result<bool> {
        match self {
            Self::Existing(_) => Ok(false),
            Self::New(path) => match std::fs::create_dir(path) {
                Ok(()) => Ok(true),
                // Created concurrently since the check: use it as it is.
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && path.is_dir() => {
                    Ok(false)
                }
                Err(e) => Err(Error::Internal(format!(
                    "cannot create folder {}: {e}",
                    path.display()
                ))),
            },
        }
    }
}

/// The folders every path-based cleanup sweep must keep (PROTOCOL §5.1
/// "Plain-folder workspaces": the folder is never deleted, moved or modified
/// by Intent). Loaded from the store — live, archived and retired
/// plain-folder rows plus deleted ones remembered on their tombstone — so it
/// covers a retried delete whose row is gone and another workspace whose
/// id-derived `<root>/<id>` candidate points at a plain folder
/// (intent-hq/intent#6229).
///
/// Every removal of an id-derived directory goes through
/// [`PlainFolderGuard::removable`]; a sweep whose guard could not be loaded
/// removes nothing (fail closed).
#[derive(Debug, Clone, Default)]
pub(crate) struct PlainFolderGuard {
    folders: Vec<PathBuf>,
    /// A protected folder could not be resolved (e.g. an alias behind an
    /// unreadable ancestor): its real location is unknown, so nothing is
    /// removable and everything counts as inside (fail closed). Never a
    /// lexical guess.
    unresolved: bool,
}

impl PlainFolderGuard {
    /// Load the guard from the store.
    ///
    /// # Errors
    ///
    /// The store error; callers then skip their sweep.
    pub(crate) async fn load(store: &intent_store::Store) -> Result<Self> {
        Ok(Self::from_paths(store.plain_folder_paths().await?))
    }

    /// A guard over `paths` (absolute folder paths).
    pub(crate) fn from_paths<I, P>(paths: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: AsRef<Path>,
    {
        let mut guard = Self::default();
        for path in paths {
            let path = path.as_ref();
            if path.as_os_str().is_empty() {
                continue;
            }
            match resolved(path) {
                Some(folder) => guard.folders.push(folder),
                None => {
                    tracing::warn!(
                        folder = %path.display(),
                        "plain-folder path cannot be resolved; path-based cleanup is paused"
                    );
                    guard.unresolved = true;
                }
            }
        }
        guard
    }

    /// Add `folder` (already a guard's own protected path) to this guard.
    pub(crate) fn with_folder(mut self, folder: &Path) -> Self {
        let extra = Self::from_paths([folder]);
        self.folders.extend(extra.folders);
        self.unresolved |= extra.unresolved;
        self
    }

    /// Whether `dir` overlaps a plain folder: it is one, holds one, or lies
    /// inside one. Paths are compared canonically (symlinks resolved) when
    /// they exist, else lexically.
    ///
    /// A `dir` whose resolution is uncertain counts as protected (fail
    /// closed).
    ///
    /// Boundary: a git workspace the user later creates from a folder that
    /// became a git repository is an ordinary git workspace. The git
    /// housekeeping its delete or retire does on that repository's `.git`
    /// (pruning the worktree registration and branch Intent created) is
    /// expected git behavior, not a plain-folder change; only directory
    /// removal and creation go through this guard.
    pub(crate) fn protects(&self, dir: &Path) -> bool {
        if self.unresolved {
            return true;
        }
        if self.folders.is_empty() {
            return false;
        }
        let Some(dir) = resolved(dir) else {
            return true;
        };
        self.folders
            .iter()
            .any(|folder| folder.starts_with(&dir) || dir.starts_with(folder))
    }

    /// Whether `dir` is a plain folder or lies inside one (creating it
    /// would write into the user's folder). Uncertain resolution counts as
    /// inside (fail closed).
    pub(crate) fn is_inside(&self, dir: &Path) -> bool {
        if self.unresolved {
            return true;
        }
        if self.folders.is_empty() {
            return false;
        }
        let Some(dir) = resolved(dir) else {
            return true;
        };
        self.folders.iter().any(|folder| dir.starts_with(folder))
    }

    /// Whether the locked checkout cleanup may touch `checkout`: the
    /// checkout itself does not overlap a plain folder, its `<root>/<id>`
    /// parent (trash sibling, empty-parent removal) does not lie inside one,
    /// and the sibling `.workspace` metadata dir does not overlap one. A
    /// parent that merely HOLDS an unrelated plain folder is fine. `false`
    /// when the guard could not be loaded (fail closed).
    pub(crate) fn checkout_removable(
        guard: &Result<Self>,
        checkout: &Path,
        sweep: &'static str,
    ) -> bool {
        let Ok(loaded) = guard else {
            return Self::removable(guard, checkout, sweep);
        };
        let Some(parent) = checkout.parent() else {
            return false;
        };
        if loaded.is_inside(parent) {
            tracing::info!(sweep, dir = %parent.display(), "keeping a checkout whose parent lies inside a plain-folder workspace");
            return false;
        }
        Self::removable(guard, checkout, sweep)
            && Self::removable(guard, &parent.join(".workspace"), sweep)
    }

    /// Fail-closed removal check for a sweep that loaded `guard`: `false`
    /// (keep `dir`, logged) when the guard could not be loaded or `dir`
    /// overlaps a plain folder.
    pub(crate) fn removable(guard: &Result<Self>, dir: &Path, sweep: &'static str) -> bool {
        match guard {
            Ok(guard) if !guard.protects(dir) => true,
            Ok(_) => {
                tracing::info!(sweep, dir = %dir.display(), "keeping a directory that overlaps a plain-folder workspace");
                false
            }
            Err(e) => {
                tracing::warn!(sweep, dir = %dir.display(), error = %e, "plain-folder lookup failed; keeping the directory");
                false
            }
        }
    }
}

fn rejected(reason: PlainFolderPathReason, path: &Path, repository_root: Option<&Path>) -> Error {
    Error::PlainFolderPathRejected(Box::new(PlainFolderPathRejection {
        reason,
        path: path.to_string_lossy().into_owned(),
        repository_root: repository_root.map(|r| r.to_string_lossy().into_owned()),
    }))
}

/// Resolve `.`/`..` components lexically (no filesystem access), for roots
/// that may not exist yet.
fn normalize_lexically(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Canonical form when the path exists, else its lexical normal form.
fn comparable(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| normalize_lexically(path))
}

/// `path` resolved with the kernel's own semantics: the longest existing
/// prefix is canonicalized as written (symlinks are followed BEFORE any
/// `..`, so `alias/../x` resolves to where the kernel would take it), then
/// the missing tail is appended. `None` when the resolution is uncertain: a
/// relative path, a missing tail holding `.`/`..`, a dangling symlink, or any
/// I/O error other than not-found. Never normalizes lexically first.
fn resolved(path: &Path) -> Option<PathBuf> {
    if !path.is_absolute() {
        return None;
    }
    let mut existing = path.to_path_buf();
    let mut missing: Vec<std::ffi::OsString> = Vec::new();
    loop {
        match std::fs::canonicalize(&existing) {
            Ok(mut out) => {
                for name in missing.into_iter().rev() {
                    out.push(name);
                }
                return Some(out);
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Present but unresolvable (a dangling symlink): uncertain.
                if existing.symlink_metadata().is_ok() {
                    return None;
                }
                match existing.components().next_back() {
                    Some(Component::Normal(name)) => missing.push(name.to_os_string()),
                    _ => return None,
                }
                if !existing.pop() {
                    return None;
                }
            }
            Err(_) => return None,
        }
    }
}

/// Nearest ancestor of `dir` (inclusive) holding a `.git` entry — a
/// directory for a repository, a file for a linked worktree or submodule.
fn enclosing_git_repo(dir: &Path) -> Option<PathBuf> {
    dir.ancestors()
        .find(|d| d.join(".git").symlink_metadata().is_ok())
        .map(Path::to_path_buf)
}

/// Check that `path` (absolute, tilde-expanded) can hold a plain folder and
/// classify it. `workspaces_roots` are the daemon workspaces root and any
/// configured worktrees location; a path equal to or inside one is refused.
/// Read-only: nothing is created.
///
/// # Errors
///
/// [`Error::PlainFolderInvalidParams`] for a relative path or one without a
/// final folder name; [`Error::PlainFolderPathRejected`] with the documented
/// `reason` otherwise.
pub(crate) fn resolve_target(
    path: &Path,
    workspaces_roots: &[PathBuf],
) -> Result<PlainFolderTarget> {
    if !path.is_absolute() {
        return Err(invalid(
            "repositoryPath must be an absolute path when plainFolder is true",
        ));
    }
    let (target, probe, resolved) = match std::fs::metadata(path) {
        Ok(meta) if meta.is_dir() => {
            let resolved = comparable(path);
            (
                PlainFolderTarget::Existing(path.to_path_buf()),
                resolved.clone(),
                resolved,
            )
        }
        Ok(_) => return Err(rejected(PlainFolderPathReason::NotADirectory, path, None)),
        Err(_) => {
            let (Some(parent), Some(name)) = (path.parent(), path.file_name()) else {
                return Err(invalid(
                    "repositoryPath must name a folder when plainFolder is true",
                ));
            };
            match std::fs::metadata(parent) {
                Ok(meta) if meta.is_dir() => {}
                Ok(_) => return Err(rejected(PlainFolderPathReason::NotADirectory, path, None)),
                Err(_) => return Err(rejected(PlainFolderPathReason::ParentMissing, path, None)),
            }
            let parent = comparable(parent);
            let resolved = parent.join(name);
            (PlainFolderTarget::New(path.to_path_buf()), parent, resolved)
        }
    };
    if workspaces_roots
        .iter()
        .map(|root| comparable(root))
        .any(|root| resolved.starts_with(&root))
    {
        return Err(rejected(
            PlainFolderPathReason::InsideWorkspacesRoot,
            path,
            None,
        ));
    }
    // A folder holding a root would put every daemon-owned checkout inside
    // the user's folder, where the data-safety guard keeps Intent from
    // creating or removing anything.
    if workspaces_roots
        .iter()
        .map(|root| comparable(root))
        .any(|root| root.starts_with(&resolved))
    {
        return Err(rejected(
            PlainFolderPathReason::ContainsWorkspacesRoot,
            path,
            None,
        ));
    }
    if let Some(repo) = enclosing_git_repo(&probe) {
        return Err(rejected(
            PlainFolderPathReason::InsideGitRepo,
            path,
            Some(&repo),
        ));
    }
    Ok(target)
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::ContextLink;

    fn plain(path: &str) -> WorkspaceCreate {
        WorkspaceCreate {
            plain_folder: Some(true),
            repository_path: Some(path.to_string()),
            ..Default::default()
        }
    }

    fn reason(err: &Error) -> PlainFolderPathReason {
        match err {
            Error::PlainFolderPathRejected(r) => r.reason,
            other => panic!("expected a path rejection, got {other:?}"),
        }
    }

    #[test]
    fn shape_ignores_requests_without_plain_folder() {
        let input = WorkspaceCreate {
            plain_folder: Some(false),
            github_url: Some("https://github.com/o/r".into()),
            ..Default::default()
        };
        validate_create_shape(&input).expect("not a plain-folder create");
        validate_create_shape(&WorkspaceCreate::default()).expect("legacy create");
    }

    #[test]
    fn shape_rejects_each_git_only_option_by_name() {
        type SetOption = fn(&mut WorkspaceCreate);
        let cases: [(&str, SetOption); 7] = [
            ("githubUrl", |i| {
                i.github_url = Some("https://github.com/o/r".into());
            }),
            ("clonePath", |i| {
                i.clone_path = Some("/tmp/x".into());
            }),
            ("isNewRepo", |i| {
                i.is_new_repo = Some(true);
            }),
            ("baseRef", |i| {
                i.base_ref = Some("main".into());
            }),
            ("branch", |i| {
                i.branch = Some("feature".into());
            }),
            ("worktreePath", |i| {
                i.worktree_path = Some("/tmp/wt".into());
            }),
            ("isRemote", |i| {
                i.is_remote = Some(true);
            }),
        ];
        for (name, set) in cases {
            let mut input = plain("/tmp/folder");
            set(&mut input);
            let err = validate_create_shape(&input).expect_err(name);
            match err {
                Error::PlainFolderInvalidParams(m) => assert!(m.contains(name), "{name}: {m}"),
                other => panic!("{name}: {other:?}"),
            }
        }
        // False / blank values are not "set".
        let mut input = plain("/tmp/folder");
        input.is_new_repo = Some(false);
        input.is_remote = Some(false);
        input.branch = Some("  ".into());
        validate_create_shape(&input).expect("unset git options pass");
    }

    #[test]
    fn shape_rejects_repository_checkout() {
        let mut input = plain("/tmp/folder");
        input.repository_checkout = Some(intent_core::repository_checkout::CheckoutSelection {
            checkout_id: "c".into(),
            revision: "1".into(),
            project_path: "g/p".into(),
            branch: "main".into(),
            commit_sha: "0123456789abcdef0123456789abcdef01234567".into(),
            mode: intent_core::repository_checkout::CheckoutMode::Direct,
        });
        let err = validate_create_shape(&input).expect_err("repositoryCheckout");
        assert!(err.to_string().contains("repositoryCheckout"), "{err}");
    }

    #[test]
    fn shape_rejects_pr_links_and_missing_path() {
        let link = |kind| ContextLink {
            kind,
            url: "https://github.com/o/r/pull/1".into(),
            owner: "o".into(),
            repo: "r".into(),
            number: 1,
        };
        let mut input = plain("/tmp/folder");
        input.context_links = Some(vec![link(ContextLinkKind::Issue)]);
        validate_create_shape(&input).expect("issue links are accepted");
        input.context_links = Some(vec![
            link(ContextLinkKind::Issue),
            link(ContextLinkKind::Pr),
        ]);
        assert!(matches!(
            validate_create_shape(&input),
            Err(Error::PlainFolderInvalidParams(_))
        ));

        for path in [None, Some(String::new()), Some("   ".to_string())] {
            let input = WorkspaceCreate {
                plain_folder: Some(true),
                repository_path: path.clone(),
                ..Default::default()
            };
            match validate_create_shape(&input) {
                Err(Error::PlainFolderInvalidParams(m)) => {
                    assert!(m.contains("repositoryPath"), "{path:?}: {m}");
                }
                other => panic!("{path:?}: {other:?}"),
            }
        }
    }

    #[test]
    fn resolves_existing_and_new_folders() {
        let tmp = crate::test_support::test_tempdir("intentd-plain-folder-");
        let existing = tmp.path().join("notes");
        std::fs::create_dir(&existing).expect("mkdir");
        assert_eq!(
            resolve_target(&existing, &[]).expect("existing"),
            PlainFolderTarget::Existing(existing.clone())
        );
        let new = tmp.path().join("fresh");
        let target = resolve_target(&new, &[]).expect("new");
        assert_eq!(target, PlainFolderTarget::New(new.clone()));
        assert!(!new.exists(), "resolving creates nothing");
        assert!(target.materialize().expect("create"));
        assert!(new.is_dir());
        assert!(!new.join(".git").exists());
        assert!(!PlainFolderTarget::Existing(existing)
            .materialize()
            .expect("noop"));
    }

    #[test]
    fn rejects_file_and_missing_parent() {
        let tmp = crate::test_support::test_tempdir("intentd-plain-folder-");
        let file = tmp.path().join("file.txt");
        std::fs::write(&file, "x").expect("write");
        assert_eq!(
            reason(&resolve_target(&file, &[]).expect_err("file")),
            PlainFolderPathReason::NotADirectory
        );
        let missing = tmp.path().join("a").join("b");
        assert_eq!(
            reason(&resolve_target(&missing, &[]).expect_err("missing parent")),
            PlainFolderPathReason::ParentMissing
        );
        assert!(!tmp.path().join("a").exists(), "nothing created");
        assert!(matches!(
            resolve_target(Path::new("relative/folder"), &[]),
            Err(Error::PlainFolderInvalidParams(_))
        ));
    }

    #[test]
    fn rejects_paths_inside_a_git_repo_with_its_root() {
        let tmp = crate::test_support::test_tempdir("intentd-plain-folder-");
        let repo = tmp.path().join("repo");
        std::fs::create_dir_all(repo.join(".git")).expect("mkdir .git");
        std::fs::create_dir_all(repo.join("docs")).expect("mkdir docs");
        let canonical_repo = std::fs::canonicalize(&repo).expect("canonical");
        for path in [repo.clone(), repo.join("docs"), repo.join("new-folder")] {
            match resolve_target(&path, &[]) {
                Err(Error::PlainFolderPathRejected(r)) => {
                    assert_eq!(r.reason, PlainFolderPathReason::InsideGitRepo, "{path:?}");
                    assert_eq!(
                        r.repository_root.as_deref(),
                        Some(canonical_repo.to_string_lossy().as_ref())
                    );
                }
                other => panic!("{path:?}: {other:?}"),
            }
        }
        // A linked worktree / submodule marks itself with a `.git` file.
        let worktree = tmp.path().join("wt");
        std::fs::create_dir(&worktree).expect("mkdir");
        std::fs::write(worktree.join(".git"), "gitdir: /elsewhere").expect("write");
        assert_eq!(
            reason(&resolve_target(&worktree, &[]).expect_err("worktree")),
            PlainFolderPathReason::InsideGitRepo
        );
        assert!(!repo.join("new-folder").exists(), "nothing created");
    }

    #[test]
    fn rejects_the_workspaces_root_and_paths_inside_it() {
        let tmp = crate::test_support::test_tempdir("intentd-plain-folder-");
        let root = tmp.path().join("workspaces");
        std::fs::create_dir_all(root.join("ws-1")).expect("mkdir");
        let roots = vec![root.clone()];
        for path in [root.clone(), root.join("ws-1"), root.join("new")] {
            assert_eq!(
                reason(&resolve_target(&path, &roots).expect_err("inside root")),
                PlainFolderPathReason::InsideWorkspacesRoot,
                "{path:?}"
            );
        }
        // A root that does not exist yet still guards its future subtree.
        let future = vec![tmp.path().join("later")];
        assert_eq!(
            reason(&resolve_target(&tmp.path().join("later"), &future).expect_err("root")),
            PlainFolderPathReason::InsideWorkspacesRoot
        );
        // A sibling sharing the name prefix is outside.
        let sibling = tmp.path().join("workspaces-notes");
        resolve_target(&sibling, &roots).expect("sibling is outside the root");
    }

    /// The guard keeps a candidate that is the plain folder, holds it, or
    /// lies inside it; leaves unrelated and prefix-sibling directories
    /// removable; resolves `..` and symlink aliases; and fails closed when
    /// the plain-folder lookup failed.
    #[test]
    fn guard_protects_overlapping_directories_and_fails_closed() {
        let tmp = crate::test_support::test_tempdir("intentd-plain-guard-");
        let folder = tmp.path().join("docs").join("my-notes");
        std::fs::create_dir_all(&folder).expect("folder");
        let guard = PlainFolderGuard::from_paths([&folder]);

        assert!(guard.protects(&folder), "the folder itself");
        assert!(guard.protects(&tmp.path().join("docs")), "an ancestor");
        assert!(
            guard.protects(&folder.join("ws-id")),
            "a missing dir inside"
        );
        assert!(
            guard.protects(
                &tmp.path()
                    .join("docs")
                    .join("x")
                    .join("..")
                    .join("my-notes")
            ),
            "a `..` alias"
        );
        assert!(
            !guard.protects(&tmp.path().join("docs").join("my-notes-2")),
            "prefix sibling"
        );
        assert!(!guard.protects(&tmp.path().join("other")), "unrelated");

        #[cfg(unix)]
        {
            let link = tmp.path().join("link");
            std::os::unix::fs::symlink(tmp.path().join("docs"), &link).expect("symlink");
            assert!(
                guard.protects(&link.join("my-notes")),
                "a symlinked alias of the folder"
            );
            assert!(
                guard.protects(&link.join("my-notes").join("ws-id")),
                "inside via a symlink"
            );
            let via_link = PlainFolderGuard::from_paths([link.join("my-notes")]);
            assert!(
                via_link.protects(&folder),
                "a folder recorded through a symlink"
            );
        }

        let ok: Result<PlainFolderGuard> = Ok(guard);
        assert!(!PlainFolderGuard::removable(&ok, &folder, "test"));
        assert!(PlainFolderGuard::removable(
            &ok,
            &tmp.path().join("other"),
            "test"
        ));
        let failed: Result<PlainFolderGuard> = Err(Error::Internal("db down".into()));
        assert!(
            !PlainFolderGuard::removable(&failed, &tmp.path().join("other"), "test"),
            "a failed lookup keeps every directory"
        );
    }

    /// Kernel semantics, not lexical: `alias/..` follows the symlink first.
    /// Uncertain resolutions (a `..` after a missing component, a dangling
    /// symlink) are protected. `is_inside` is one-directional.
    #[cfg(unix)]
    #[test]
    fn guard_resolves_like_the_kernel_and_fails_closed_when_uncertain() {
        let tmp = crate::test_support::test_tempdir("intentd-plain-guard-k-");
        let t = tmp.path();
        std::fs::create_dir_all(t.join("real").join("sub")).expect("real/sub");
        std::fs::create_dir_all(t.join("real").join("victim")).expect("victim");
        std::os::unix::fs::symlink(t.join("real").join("sub"), t.join("alias")).expect("alias");
        std::os::unix::fs::symlink(t.join("nowhere"), t.join("dangling")).expect("dangling");
        let guard = PlainFolderGuard::from_paths([t.join("real").join("victim")]);

        assert!(guard.protects(&t.join("alias").join("..").join("victim")));
        assert!(guard.protects(&t.join("alias").join("..").join("victim").join("cache")));
        assert!(!guard.protects(&t.join("alias").join("..").join("other")));
        assert!(
            !guard.protects(&t.join("victim")),
            "lexical lookalike is a different dir"
        );
        assert!(
            guard.protects(&t.join("missing").join("..").join("x")),
            "uncertain"
        );
        assert!(
            guard.protects(&t.join("dangling")),
            "dangling symlink is uncertain"
        );
        assert!(
            guard.protects(Path::new("relative/dir")),
            "relative is uncertain"
        );

        assert!(guard.is_inside(&t.join("real").join("victim").join("new")));
        assert!(guard.is_inside(&t.join("alias").join("..").join("victim")));
        assert!(
            !guard.is_inside(&t.join("real")),
            "holding a plain folder is not inside"
        );
    }

    /// A protected folder that cannot be resolved (relative here; an alias
    /// behind an unreadable ancestor in practice) puts the guard in a
    /// fail-closed state: nothing is removable and everything counts as
    /// inside, never a lexical guess.
    #[test]
    fn guard_fails_closed_when_a_protected_folder_cannot_be_resolved() {
        let tmp = crate::test_support::test_tempdir("intentd-plain-guard-u-");
        let elsewhere = tmp.path().join("elsewhere");
        let guard = PlainFolderGuard::from_paths([Path::new("relative/notes")]);
        assert!(guard.protects(&elsewhere));
        assert!(guard.is_inside(&elsewhere));
        let loaded: Result<PlainFolderGuard> = Ok(guard);
        assert!(!PlainFolderGuard::removable(&loaded, &elsewhere, "test"));
        assert!(!PlainFolderGuard::checkout_removable(
            &loaded,
            &elsewhere.join("ws").join("repo"),
            "test"
        ));
    }

    /// Same with a real unreadable alias ancestor (skipped when running with
    /// privileges that ignore directory permissions).
    #[cfg(unix)]
    #[test]
    fn guard_fails_closed_for_an_alias_behind_an_unreadable_ancestor() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = crate::test_support::test_tempdir("intentd-plain-guard-l-");
        let accessible = tmp.path().join("accessible");
        let locked = tmp.path().join("locked");
        std::fs::create_dir_all(accessible.join("victim")).expect("victim");
        std::fs::create_dir_all(&locked).expect("locked");
        std::os::unix::fs::symlink(accessible.join("victim"), locked.join("link")).expect("link");
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).expect("chmod");
        let unreadable = std::fs::canonicalize(locked.join("link")).is_err();
        let guard = PlainFolderGuard::from_paths([locked.join("link")]);
        let protected = guard.protects(&accessible.join("victim"));
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755))
            .expect("chmod back");
        if unreadable {
            assert!(protected, "the real folder stays protected");
        }
    }
}
