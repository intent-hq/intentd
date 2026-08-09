//! Hidden, daemon-managed cache of read-only GitHub clones.
//!
//! Layout: `<cache_root>/<owner>/<repo>` — a normal clone with the remote's
//! default branch checked out. The caller passes the cache root (e.g.
//! `<workspaces_root>/.repo-cache`, dot-prefixed so it stays invisible to
//! users and recent-repo derivation); this module never reads config.
//!
//! [`ensure_cached_repo`] is the single entry point: it serializes callers on
//! a per-repo async lock, then either clones fresh (cache miss) or refreshes
//! the existing cache (`git fetch --prune origin` + hard reset to the remote
//! default branch). Submodules are part of the cached state: the clone
//! recurses into them and the refresh syncs, force-updates, and cleans them,
//! so hydrated checkouts always copy populated, pristine submodule work
//! trees. A refresh anomaly — diverged history, corrupt object store, an
//! interrupted prior clone, a vanished `origin/HEAD`, a broken submodule —
//! never fails the flow: the cache dir is deleted and re-cloned from scratch.
//! Only a failed *clone* (nothing left to fall back to) surfaces as an error.
//!
//! Network git work shells out to system `git` (same rationale as
//! [`crate::fetch`]): the child inherits OpenSSH config + credential-helper
//! resolution, `GIT_TERMINAL_PROMPT=0` fails fast instead of prompting, and a
//! wall-clock deadline kills a hung child. A caller-resolved GitHub token is
//! offered via the env-backed github.com-scoped credential helper
//! ([`crate::auth::token_helper_config`]) — never argv.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use git2::Repository;
use intent_core::{Error, Result};
use tokio::sync::Mutex as AsyncMutex;

use crate::auth::{token_helper_config, TOKEN_ENV};
use crate::map_git_err;

/// Wall-clock bound for the cache clone, matching the service-layer clone
/// budget (`intent-services` CLONE_TIMEOUT).
const CACHE_CLONE_TIMEOUT: Duration = Duration::from_secs(300);

/// Wall-clock bound for the refresh fetch, matching [`crate::fetch`]'s
/// SHELL_FETCH_TIMEOUT.
const CACHE_FETCH_TIMEOUT: Duration = Duration::from_secs(100);

/// Poll interval while waiting for a shelled-out git child to exit.
const GIT_POLL: Duration = Duration::from_millis(50);

/// Process-global per-repo lock registry, keyed by cache path. Concurrent
/// workspace creates for the same repo serialize their refresh/clone here;
/// different repos never contend.
fn repo_locks() -> &'static Mutex<HashMap<PathBuf, Arc<AsyncMutex<()>>>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<AsyncMutex<()>>>>> = OnceLock::new();
    LOCKS.get_or_init(Default::default)
}

/// Resolve (or create) the async lock for `cache_path`.
fn lock_for(cache_path: &Path) -> Arc<AsyncMutex<()>> {
    let mut map = repo_locks().lock().expect("repo cache lock map poisoned");
    map.entry(cache_path.to_path_buf()).or_default().clone()
}

/// Ensure `<cache_root>/<owner>/<repo>` holds a fresh cached clone of
/// `github_url` and return that path.
///
/// - Cache miss: full clone into the cache path.
/// - Cache hit: `git fetch --prune origin` + hard reset of the remote default
///   branch. Any anomaly self-heals by deleting the cache dir and re-cloning —
///   refresh never fails the flow.
///
/// `token` is an optional caller-resolved GitHub token offered to the child
/// git via the environment (see [`crate::auth`]); it never appears in argv.
/// Callers for the same repo serialize on a per-repo async lock; the git work
/// itself runs on the blocking pool.
pub async fn ensure_cached_repo(
    cache_root: &Path,
    github_url: &str,
    owner: &str,
    repo: &str,
    token: Option<&str>,
) -> Result<PathBuf> {
    validate_segment("owner", owner)?;
    validate_segment("repo", repo)?;
    let cache_path = cache_root.join(owner).join(repo);

    let lock = lock_for(&cache_path);
    let _guard = lock.lock().await;

    let path = cache_path.clone();
    let url = github_url.to_string();
    let token = token.map(str::to_owned);
    tokio::task::spawn_blocking(move || ensure_blocking(&path, &url, token.as_deref()))
        .await
        .map_err(|e| Error::Internal(format!("repo cache task failed: {e}")))??;
    Ok(cache_path)
}

/// Blocking body of [`ensure_cached_repo`]: refresh when a cache is present,
/// otherwise (or when the refresh reports any anomaly) wipe and re-clone.
fn ensure_blocking(cache_path: &Path, github_url: &str, token: Option<&str>) -> Result<()> {
    if cache_path.exists() {
        if origin_matches(cache_path, github_url) {
            match refresh(cache_path, token) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        path = %cache_path.display(),
                        "repo cache refresh failed; deleting cache and re-cloning"
                    );
                }
            }
        } else {
            // The cache is keyed by `<owner>/<repo>` segments only, so two
            // different hosts (or two `file://` sources) carrying the same
            // owner/repo pair must never serve each other's content. A cache
            // whose `origin` differs from the requested URL is stale, not a
            // hit — wipe and re-clone from the requested URL.
            tracing::warn!(
                path = %cache_path.display(),
                "repo cache origin does not match the requested URL; re-cloning"
            );
        }
    }
    remove_cache_path(cache_path)?;
    clone(github_url, cache_path, token)
}

/// Whether the cache's `origin` remote points at exactly `github_url`. Any
/// failure to read it (unopenable repo, missing remote) counts as a mismatch
/// — the caller self-heals by re-cloning.
fn origin_matches(cache_path: &Path, github_url: &str) -> bool {
    let Ok(repo) = Repository::open(cache_path) else {
        return false;
    };
    let Ok(remote) = repo.find_remote("origin") else {
        return false;
    };
    remote.url().ok() == Some(github_url)
}

/// Refresh an existing cache: fetch + prune, re-resolve the remote's default
/// branch (`git remote set-head origin --auto` — a fetch alone never updates
/// the `origin/HEAD` symref recorded at clone time, so a remote that changed
/// its default branch would otherwise pin the cache to the obsolete one),
/// then hard-reset that branch, sync every submodule work tree to its
/// recorded gitlink, drop untracked files so the work tree exactly
/// mirrors the remote — a diverged, dirty, or polluted cache is clobbered,
/// never merged — and prune module git dirs the current `.gitmodules` no
/// longer names. Every failure here is an anomaly the caller self-heals by
/// re-cloning.
fn refresh(cache_path: &Path, token: Option<&str>) -> Result<()> {
    run_git(
        cache_path,
        &["fetch", "--prune", "origin"],
        token,
        CACHE_FETCH_TIMEOUT,
    )?;
    run_git(
        cache_path,
        &["remote", "set-head", "origin", "--auto"],
        token,
        CACHE_FETCH_TIMEOUT,
    )?;
    let repo = Repository::open(cache_path).map_err(map_git_err)?;
    let default = default_branch(&repo)?;
    let target = repo
        .find_reference(&format!("refs/remotes/origin/{default}"))
        .map_err(map_git_err)?
        .target()
        .ok_or_else(|| Error::Internal(format!("origin/{default} has no target")))?;
    repo.reference(
        &format!("refs/heads/{default}"),
        target,
        true,
        "repo-cache refresh",
    )
    .map_err(map_git_err)?;
    repo.set_head(&format!("refs/heads/{default}"))
        .map_err(map_git_err)?;
    drop(repo);
    crate::reset::reset_hard(cache_path, "HEAD")?;
    // Submodules (checked after the reset so `.gitmodules` reflects the new
    // tip; skipped cheaply when there is none): `sync` re-points recorded
    // URLs at what `.gitmodules` now says, then `update --force` moves every
    // submodule work tree to the recorded gitlink (`--force` subsumes a
    // per-submodule hard reset). Network can occur here — a bumped gitlink
    // needs the new commit — so the token is offered.
    let has_submodules = crate::submodule::has_submodules(cache_path);
    if has_submodules {
        run_git(
            cache_path,
            &["submodule", "sync", "--recursive"],
            token,
            CACHE_FETCH_TIMEOUT,
        )?;
        run_git(
            cache_path,
            &["submodule", "update", "--init", "--recursive", "--force"],
            token,
            CACHE_FETCH_TIMEOUT,
        )?;
    }
    // Untracked pollution (e.g. leftovers from a process killed mid-checkout
    // in the cache) survives a hard reset and would be byte-copied into every
    // hydrated checkout; clean it so refresh restores a pristine work tree.
    // Double `-f` so an orphaned submodule checkout — a nested repo left
    // behind when upstream removed the submodule — is removable too.
    run_git(cache_path, &["clean", "-ffdx"], None, CACHE_FETCH_TIMEOUT)?;
    if has_submodules {
        // Untracked pollution inside live submodule work trees is invisible
        // to the superproject clean; drop it per submodule.
        run_git(
            cache_path,
            &[
                "submodule",
                "foreach",
                "--recursive",
                "git",
                "clean",
                "-fdx",
            ],
            None,
            CACHE_FETCH_TIMEOUT,
        )?;
    }
    // Stale module git dirs: when upstream removes (or renames) a submodule,
    // its `.git/modules/<name>` dir — created by an earlier clone/refresh —
    // survives the reset and the cleans (which never enter `.git`) and would
    // be byte-copied into every direct-hydrated checkout. Prune module dirs
    // the current `.gitmodules` no longer names, after the cleans so the
    // orphaned work tree's gitfile never dangles mid-refresh. An error here
    // is a refresh anomaly like any other (the caller wipes and re-clones).
    prune_stale_modules(cache_path)?;
    Ok(())
}

/// Resolve the remote's default branch from the `refs/remotes/origin/HEAD`
/// symref `git clone` records. A cache where the symref vanished or is not
/// symbolic is an anomaly — the caller re-clones (which re-creates it).
fn default_branch(repo: &Repository) -> Result<String> {
    let head = repo
        .find_reference("refs/remotes/origin/HEAD")
        .map_err(map_git_err)?;
    let target = head
        .symbolic_target()
        .map_err(map_git_err)?
        .ok_or_else(|| Error::Internal("origin/HEAD is not a symbolic ref".to_string()))?;
    target
        .strip_prefix("refs/remotes/origin/")
        .map(str::to_owned)
        .ok_or_else(|| Error::Internal(format!("unexpected origin/HEAD target: {target}")))
}

/// Fresh clone of `github_url` into `cache_path` (parent dirs created first).
/// A plain clone with `--recurse-submodules` — the remote's default branch
/// ends up checked out, `origin/HEAD` recorded, and every submodule work tree
/// populated at its recorded gitlink, exactly the state [`refresh`] relies
/// on. [`CACHE_CLONE_TIMEOUT`] bounds the whole clone, submodules included.
fn clone(github_url: &str, cache_path: &Path, token: Option<&str>) -> Result<()> {
    if let Some(parent) = cache_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| Error::Internal(format!("failed to create cache dir: {e}")))?;
    }
    // `git -C <parent> clone <url> <dir-name>` so no argument is ever
    // interpreted relative to this process's cwd.
    let parent = cache_path
        .parent()
        .ok_or_else(|| Error::Internal("cache path has no parent".to_string()))?;
    let dir_name = cache_path
        .file_name()
        .ok_or_else(|| Error::Internal("cache path has no file name".to_string()))?;
    // `--` so a caller-supplied URL starting with `-` can never be parsed as
    // a git option (option injection).
    let args: Vec<&std::ffi::OsStr> = vec![
        "clone".as_ref(),
        "--recurse-submodules".as_ref(),
        "--".as_ref(),
        github_url.as_ref(),
        dir_name,
    ];
    run_git_os(parent, &args, token, CACHE_CLONE_TIMEOUT).inspect_err(|_| {
        // A failed clone must not leave a half-written cache behind for the
        // next caller's refresh to trip over.
        let _ = std::fs::remove_dir_all(cache_path);
    })
}

/// Run blocking work under the per-repo cache lock for `cache_path`, so a
/// checkout provisioned FROM the cache never overlaps a concurrent
/// [`ensure_cached_repo`] refresh/re-clone of the same cache (which
/// hard-resets or deletes the directory mid-read). The closure runs on the
/// blocking pool.
pub async fn with_cache_lock_blocking<T, F>(cache_path: &Path, f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    let lock = lock_for(cache_path);
    let _guard = lock.lock().await;
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::Internal(format!("repo cache task failed: {e}")))?
}

/// Provision a **standalone** plain-clone checkout from a cached repo (the
/// `direct` fallback of `workspace.create` cache hydration, used when the
/// filesystem cannot CoW-clone the cache into the workspaces root).
///
/// Steps (all local, no network):
/// 1. `git clone <cache_path> <checkout_path>` — a plain local clone
///    (hardlinked objects where the filesystem allows).
/// 2. Copy the cache's remote-tracking refs (`refs/remotes/origin/*`, the
///    GitHub branches) into the clone so `base_ref` resolution sees every
///    upstream branch, not just the cache's default.
/// 3. Create + check out `branch` from `base_ref` (same resolution and
///    branch-reuse semantics as the CoW checkout path) and hard-reset to it.
/// 4. Retarget `origin` from the cache path to `origin_url` (the real GitHub
///    URL), so pushes/fetches in the checkout never touch the cache.
/// 5. Populate submodules from the cache's local module git dirs
///    ([`hydrate_submodules_from_cache`]) — best-effort: a submodule anomaly
///    degrades to unpopulated submodules with a warning, never a failed
///    provisioning.
///
/// Returns the SHA the checkout lands on. On failure after the clone, the
/// partially provisioned `checkout_path` is removed best-effort. Blocking —
/// callers run it on the blocking pool.
pub fn provision_direct_checkout(
    cache_path: &Path,
    checkout_path: &Path,
    origin_url: &str,
    branch: &str,
    base_ref: Option<&str>,
) -> Result<String> {
    let sha = provision_plain_clone_checkout(
        cache_path,
        checkout_path,
        OriginTarget::Url(origin_url),
        branch,
        base_ref,
    )?;
    if let Err(e) = hydrate_submodules_from_cache(cache_path, checkout_path) {
        tracing::warn!(
            checkout = %checkout_path.display(),
            cache = %cache_path.display(),
            error = %e,
            "provision_direct_checkout: submodule population failed; checkout provisioned with unpopulated submodules"
        );
    }
    Ok(sha)
}

/// Populate the checkout's submodules using only the cache's local objects.
///
/// A plain local clone carries the superproject alone: gitlink paths are
/// empty directories and `.git/modules` does not exist. Copy the cache's
/// `.git/modules` tree (the submodule git dirs the cache clone/refresh keeps
/// populated) into the checkout, then run `git submodule update --init
/// --recursive --force`: for each submodule git already finds
/// `.git/modules/<name>`, so it reconnects the work tree to it and checks out
/// the recorded gitlink from local objects instead of cloning over the
/// network. Nested submodules sit inside the copied tree at their expected
/// paths, so the recursion stays local too. The update runs strictly offline
/// ([`update_checkout_submodules`]): a gitlink the cache does not hold (or a
/// submodule with no copied git dir) fails the update and degrades to the
/// caller's warning path instead of silently fetching from the network.
///
/// `submodule init` (the `--init` half) records `submodule.<name>.url` from
/// `.gitmodules`, resolved against the checkout's `origin` — already
/// retargeted at the real URL — and the closing `submodule sync --recursive`
/// re-points every submodule's own `remote.origin.url` at the same
/// resolution, so no config value references the cache and deleting the cache
/// never breaks the checkout. Skipped entirely (no subprocess) when the
/// checkout has no `.gitmodules`.
///
/// The copy is filtered to **live** top-level modules only (the same
/// liveness rule as the refresh prune, seeded from the checkout's
/// `.gitmodules`), so a cache still carrying a dead module dir — e.g. one
/// refreshed before pruning existed — never leaks it into the checkout.
/// Nested subtrees copy wholesale: the cache work trees sit at the tip,
/// and a checkout at an older `base_ref` can select a gitlink that still
/// names a nested module the tip dropped, so nested liveness is only
/// decidable after the update populates the checkout's own work trees —
/// the closing [`prune_stale_modules`] then removes the dead nested dirs
/// the copy kept. A liveness/copy/prune failure follows the caller's
/// existing degrade-with-warning path.
fn hydrate_submodules_from_cache(cache_path: &Path, checkout_path: &Path) -> Result<()> {
    if !crate::submodule::has_submodules(checkout_path) {
        return Ok(());
    }
    let src = cache_path.join(".git").join("modules");
    let dst = checkout_path.join(".git").join("modules");
    if src.is_dir() && !dst.exists() {
        let copied = match collect_module_liveness(checkout_path, false)? {
            Some(live) => copy_modules_subdir(&src, &dst, Path::new(""), &live),
            // A module name/path we cannot map conservatively: copy
            // everything rather than risk dropping a live module.
            None => copy_dir_recursive(&src, &dst),
        };
        copied.map_err(|e| {
            Error::Internal(format!(
                "cannot copy submodule git dirs from the cache: {e}"
            ))
        })?;
    }
    update_checkout_submodules(checkout_path)?;
    // Only now do the checkout's work trees match its checked-out ref, so
    // nested liveness is decidable: drop the dead nested module dirs the
    // wholesale copy above kept.
    prune_stale_modules(checkout_path)?;
    sync_submodule_urls(checkout_path)
}

/// Re-point recorded submodule URLs (`submodule.<name>.url` in the
/// checkout's config and each submodule's own `remote.origin.url`) at what
/// `.gitmodules` currently says (`git submodule sync --recursive`). The
/// shared closing step of both hydration paths
/// ([`hydrate_submodules_from_cache`] and
/// [`crate::cow_checkout::provision_cow_checkout`]) so no checkout keeps a
/// URL copied from its source. No token: this rewrites local config only.
pub(crate) fn sync_submodule_urls(checkout_path: &Path) -> Result<()> {
    run_git(
        checkout_path,
        &["submodule", "sync", "--recursive"],
        None,
        CACHE_FETCH_TIMEOUT,
    )
}

/// Force-sync every submodule work tree in `checkout_path` to its recorded
/// gitlink (`git submodule update --init --recursive --force`). Strictly
/// offline: `--no-fetch` disables the missing-object fetch fallback and
/// `protocol.allow=never` refuses every transport a `--init` clone could
/// use (both propagate to nested submodules via `GIT_CONFIG_PARAMETERS`),
/// so a gitlink the module git dirs do not hold fails the update — callers
/// degrade with a warning — instead of silently contacting the recorded
/// URL. A `protocol.<name>.allow` set in the environment (tests allow
/// `file`) still overrides the general policy. Shared with
/// [`crate::cow_checkout::provision_cow_checkout`], whose byte copy carries
/// the source's module git dirs but whose hard reset never touches submodule
/// work trees. No token: these paths never touch the network.
pub(crate) fn update_checkout_submodules(checkout_path: &Path) -> Result<()> {
    run_git(
        checkout_path,
        &[
            "-c",
            "protocol.allow=never",
            "submodule",
            "update",
            "--init",
            "--recursive",
            "--force",
            "--no-fetch",
        ],
        None,
        CACHE_FETCH_TIMEOUT,
    )
}

/// Plain recursive directory copy (dirs, files, symlinks recreated). The
/// submodule git dirs this copies are not CoW-cloneable as a unit here (the
/// destination `.git` already exists), and correctness beats sharing for
/// them.
fn copy_dir_recursive(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let to = dst.join(entry.file_name());
        if ty.is_dir() {
            copy_dir_recursive(&entry.path(), &to)?;
        } else {
            copy_dir_entry(&entry, &ty, &to)?;
        }
    }
    Ok(())
}

/// Copy one non-directory dir entry (file or symlink) to `to`.
fn copy_dir_entry(
    entry: &std::fs::DirEntry,
    ty: &std::fs::FileType,
    to: &Path,
) -> std::io::Result<()> {
    if ty.is_symlink() {
        #[cfg(unix)]
        std::os::unix::fs::symlink(std::fs::read_link(entry.path())?, to)?;
        #[cfg(not(unix))]
        {
            std::fs::copy(entry.path(), to)?;
        }
    } else {
        std::fs::copy(entry.path(), to)?;
    }
    Ok(())
}

/// Live submodules at one `.git/modules` nesting level: each module's git
/// dir path relative to that level's `modules/` dir (the `.gitmodules`
/// module *name*, one directory component per `/`-separated segment) mapped
/// to the liveness of the module's own nested `modules/` subtree. `None`
/// marks a live module whose nesting is opaque — its work tree could not be
/// read, or nested liveness was deliberately not judged (the hydration
/// copy) — so its subtree is kept (or copied) wholesale rather than guessed
/// at.
#[derive(Default)]
struct ModuleLiveness {
    modules: BTreeMap<PathBuf, Option<ModuleLiveness>>,
}

/// Read the live module set from `root/.gitmodules`, recursing into nested
/// submodules through the work trees under `root` when `recurse_nested` is
/// set — valid only when those work trees sit at the same ref as the
/// `.gitmodules` chain being read (the refresh prune on the cache, the
/// post-update prune on a checkout). With `recurse_nested` unset every
/// module's nesting is opaque: the hydration copy filter uses this because
/// at copy time the only populated work trees are the cache's, which sit at
/// the tip — a checkout at an older `base_ref` can select a gitlink that
/// still names a nested module the tip dropped, so judging nested liveness
/// from the tip could drop a live module's git dir.
///
/// Returns `Ok(None)` when a module name or path cannot be mapped to a
/// module dir safely (non-UTF-8, absolute, `..`, …): the caller must then
/// treat the whole level as opaque and keep every dir — a kept dead dir is
/// harmless, a wrongly-pruned live one is not. A missing `.gitmodules`
/// yields an empty (all-dead) set; an unreadable one is an error.
fn collect_module_liveness(root: &Path, recurse_nested: bool) -> Result<Option<ModuleLiveness>> {
    let gitmodules = root.join(".gitmodules");
    if !gitmodules.exists() {
        return Ok(Some(ModuleLiveness::default()));
    }
    let cfg = git2::Config::open(&gitmodules).map_err(map_git_err)?;
    let mut modules = BTreeMap::new();
    let mut entries = cfg.entries(None).map_err(map_git_err)?;
    while let Some(entry) = entries.next() {
        let entry = entry.map_err(map_git_err)?;
        let Ok(name) = entry.name() else {
            return Ok(None);
        };
        let Some(module) = name
            .strip_prefix("submodule.")
            .and_then(|n| n.strip_suffix(".path"))
        else {
            continue;
        };
        let Ok(worktree_rel) = entry.value() else {
            return Ok(None);
        };
        let (Some(module_dir), Some(worktree_rel)) =
            (safe_rel_path(module), safe_rel_path(worktree_rel))
        else {
            return Ok(None);
        };
        let worktree = root.join(worktree_rel);
        let nested = if recurse_nested && worktree.is_dir() {
            collect_module_liveness(&worktree, true)?
        } else {
            None
        };
        modules.insert(module_dir, nested);
    }
    Ok(Some(ModuleLiveness { modules }))
}

/// `s` as a relative path of plain (`Normal`) components only, or `None`
/// when it is empty or carries anything (`..`, a root, a leading `./`) that
/// could make it name something outside — or alias something inside — the
/// modules dir (or, for [`crate::cow_checkout`]'s orphan cleanup, the
/// checkout work tree).
pub(crate) fn safe_rel_path(s: &str) -> Option<PathBuf> {
    let p = Path::new(s);
    p.components().next()?;
    p.components()
        .all(|c| matches!(c, std::path::Component::Normal(_)))
        .then(|| p.components().collect())
}

/// Prune dead module git dirs from a work tree's `.git/modules` tree: every
/// dir the current `.gitmodules` chain no longer names as a module (or an
/// ancestor component of one) is removed. A liveness set that cannot be
/// mapped safely keeps everything — never guess a live module away. Used on
/// the cache after a refresh and shared with [`crate::cow_checkout`], whose
/// byte copy carries the source's module dirs even when the checked-out ref
/// no longer registers them.
pub(crate) fn prune_stale_modules(cache_path: &Path) -> Result<()> {
    let modules_dir = cache_path.join(".git").join("modules");
    if !modules_dir.is_dir() {
        return Ok(());
    }
    match collect_module_liveness(cache_path, true)? {
        Some(live) => prune_modules_subdir(&modules_dir, Path::new(""), &live),
        None => Ok(()),
    }
}

/// Depth-first prune of one `modules/` level: a dir naming a live module is
/// kept (recursing into its nested `modules/` subtree when its liveness is
/// known), a dir that is an ancestor component of a live multi-segment
/// module name is descended into, and anything else is a dead module's git
/// dir — removed. Non-directory entries are left alone.
fn prune_modules_subdir(fs_dir: &Path, rel: &Path, live: &ModuleLiveness) -> Result<()> {
    let io_err =
        |e: std::io::Error| Error::Internal(format!("cannot prune stale module dirs: {e}"));
    for entry in std::fs::read_dir(fs_dir).map_err(io_err)? {
        let entry = entry.map_err(io_err)?;
        if !entry.file_type().map_err(io_err)?.is_dir() {
            continue;
        }
        let child_rel = rel.join(entry.file_name());
        match live.modules.get(&child_rel) {
            Some(Some(nested)) => {
                let nested_modules = entry.path().join("modules");
                if nested_modules.is_dir() {
                    prune_modules_subdir(&nested_modules, Path::new(""), nested)?;
                }
            }
            // Live but opaque nesting: keep the subtree wholesale.
            Some(None) => {}
            None => {
                if live.modules.keys().any(|m| m.starts_with(&child_rel)) {
                    prune_modules_subdir(&entry.path(), &child_rel, live)?;
                } else {
                    std::fs::remove_dir_all(entry.path()).map_err(io_err)?;
                }
            }
        }
    }
    Ok(())
}

/// Filtered counterpart of [`copy_dir_recursive`] for one `modules/` level:
/// live module dirs are copied (their nested `modules/` subtree filtered in
/// turn when its liveness is known), ancestor components of live
/// multi-segment module names are descended into, dead module dirs are
/// skipped. Non-directory entries copy as-is.
fn copy_modules_subdir(
    src: &Path,
    dst: &Path,
    rel: &Path,
    live: &ModuleLiveness,
) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let to = dst.join(entry.file_name());
        if !ty.is_dir() {
            copy_dir_entry(&entry, &ty, &to)?;
            continue;
        }
        let child_rel = rel.join(entry.file_name());
        match live.modules.get(&child_rel) {
            Some(Some(nested)) => copy_module_dir(&entry.path(), &to, nested)?,
            Some(None) => copy_dir_recursive(&entry.path(), &to)?,
            None => {
                if live.modules.keys().any(|m| m.starts_with(&child_rel)) {
                    copy_modules_subdir(&entry.path(), &to, &child_rel, live)?;
                }
            }
        }
    }
    Ok(())
}

/// Copy one live module's git dir, filtering only its nested `modules/`
/// subtree through `nested` — everything else (objects, refs, config, …)
/// copies verbatim.
fn copy_module_dir(src: &Path, dst: &Path, nested: &ModuleLiveness) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        let to = dst.join(entry.file_name());
        if ty.is_dir() && entry.file_name() == "modules" {
            copy_modules_subdir(&entry.path(), &to, Path::new(""), nested)?;
        } else if ty.is_dir() {
            copy_dir_recursive(&entry.path(), &to)?;
        } else {
            copy_dir_entry(&entry, &ty, &to)?;
        }
    }
    Ok(())
}

/// What the provisioned clone's `origin` remote must point at once the local
/// clone has served its purpose as the object source. Either way the clone
/// never keeps a reference to `source_path`.
pub(crate) enum OriginTarget<'a> {
    /// Retarget `origin` at this URL (the real upstream).
    Url(&'a str),
    /// Drop the `origin` remote: the source has no upstream of its own, so
    /// leaving the clone's `origin` in place would pin it to the source path.
    Remove,
}

/// Shared body of the standalone plain-clone provisioners: local `git clone`
/// of `source_path`, overlay of the source's remote-tracking refs, branch +
/// checkout + hard reset, then `origin` retargeted per `origin`. The origin
/// step runs last because [`OriginTarget::Remove`] drops the remote's
/// tracking refs along with it, and `base_ref` resolution needs them.
///
/// Returns the SHA the checkout lands on. On failure after the clone, the
/// partially provisioned `checkout_path` is removed best-effort. Blocking —
/// callers run it on the blocking pool.
pub(crate) fn provision_plain_clone_checkout(
    source_path: &Path,
    checkout_path: &Path,
    origin: OriginTarget<'_>,
    branch: &str,
    base_ref: Option<&str>,
) -> Result<String> {
    if let Some(parent) = checkout_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| Error::Internal(format!("cannot create checkout parent dir: {e}")))?;
    }
    let parent = checkout_path
        .parent()
        .ok_or_else(|| Error::Internal("checkout path has no parent".to_string()))?;
    let dir_name = checkout_path
        .file_name()
        .ok_or_else(|| Error::Internal("checkout path has no file name".to_string()))?;
    let args: Vec<&std::ffi::OsStr> = vec![
        "clone".as_ref(),
        "--".as_ref(),
        source_path.as_os_str(),
        dir_name,
    ];
    run_git_os(parent, &args, None, CACHE_CLONE_TIMEOUT)?;
    (|| {
        // The local clone only maps the source's refs/heads/* into
        // refs/remotes/origin/*; overlay the source's own remote-tracking refs
        // so every upstream branch resolves as a base ref.
        run_git(
            checkout_path,
            &[
                "fetch",
                "origin",
                "+refs/remotes/origin/*:refs/remotes/origin/*",
            ],
            None,
            CACHE_FETCH_TIMEOUT,
        )?;
        let sha =
            crate::cow_checkout::checkout_in_clone(checkout_path, branch, base_ref, "origin")?;
        let repo = Repository::open(checkout_path).map_err(map_git_err)?;
        match origin {
            OriginTarget::Url(url) => repo.remote_set_url("origin", url).map_err(map_git_err)?,
            OriginTarget::Remove => repo.remote_delete("origin").map_err(map_git_err)?,
        }
        Ok(sha)
    })()
    .inspect_err(|_| {
        let _ = std::fs::remove_dir_all(checkout_path);
    })
}

/// Delete the cache dir (self-heal / pre-clone cleanup). Missing path is fine.
fn remove_cache_path(cache_path: &Path) -> Result<()> {
    match std::fs::remove_dir_all(cache_path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(Error::Internal(format!(
            "failed to remove repo cache dir: {e}"
        ))),
    }
}

/// Reject `owner`/`repo` values that would escape the cache root, collapse
/// path segments, or read as a git option (leading `-`) — they come from
/// external input (GitHub URLs / API payloads).
fn validate_segment(what: &str, value: &str) -> Result<()> {
    let bad = value.is_empty()
        || value == "."
        || value == ".."
        || value.starts_with('-')
        || value.contains('/')
        || value.contains('\\')
        || value.contains('\0');
    if bad {
        return Err(Error::InvalidParams(format!(
            "invalid repo cache {what} segment: {value:?}"
        )));
    }
    Ok(())
}

/// Shell out to `git -C <dir> <args…>` with the fail-fast/deadline-kill
/// semantics of [`crate::fetch`]: `GIT_TERMINAL_PROMPT=0`, discarded stdout,
/// piped stderr for the error message, and a poll loop that kills the child
/// at `timeout`. The optional token travels only via [`TOKEN_ENV`].
fn run_git(dir: &Path, args: &[&str], token: Option<&str>, timeout: Duration) -> Result<()> {
    let os_args: Vec<&std::ffi::OsStr> = args.iter().map(|a| a.as_ref()).collect();
    run_git_os(dir, &os_args, token, timeout)
}

fn run_git_os(
    dir: &Path,
    args: &[&std::ffi::OsStr],
    token: Option<&str>,
    timeout: Duration,
) -> Result<()> {
    let mut cmd = Command::new("git");
    cmd.arg("-C").arg(dir);
    // Offer the resolved token as an extra github.com-scoped credential
    // helper, appended after any configured helpers (see `crate::auth`). The
    // helper reads the secret from the environment — argv carries no token.
    if let Some(token) = crate::auth::usable_token(token) {
        cmd.arg("-c").arg(token_helper_config());
        cmd.env(TOKEN_ENV, token);
    }
    let mut child = cmd
        .args(args)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| Error::Internal(format!("failed to spawn git: {e}")))?;

    // Drain stderr concurrently: clone/fetch progress and diagnostics can
    // fill the pipe on larger repos, and an undrained pipe blocks the child
    // forever — the poll loop below would then kill a healthy child at the
    // deadline.
    let drain = child.stderr.take().map(|mut stderr| {
        std::thread::spawn(move || {
            use std::io::Read;
            let mut buf = String::new();
            let _ = stderr.read_to_string(&mut buf);
            buf
        })
    });
    let read_stderr = |drain: Option<std::thread::JoinHandle<String>>| {
        drain.and_then(|h| h.join().ok()).unwrap_or_default()
    };

    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if status.success() {
                    return Ok(());
                }
                // Redact before embedding: git stderr routinely echoes the
                // remote URL, which may carry userinfo credentials; this
                // message travels into logs, events, and JSON-RPC errors.
                let stderr = crate::redact::redact_credentials(&read_stderr(drain));
                return Err(Error::Internal(format!(
                    "git {} failed: {}",
                    subcommand_name(args),
                    stderr.trim()
                )));
            }
            Ok(None) => {
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(Error::Internal(format!(
                        "git {} timed out after {}s",
                        subcommand_name(args),
                        timeout.as_secs()
                    )));
                }
                std::thread::sleep(GIT_POLL);
            }
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(Error::Internal(format!("git wait failed: {e}")));
            }
        }
    }
}

/// The git subcommand in `args` for error attribution: the first argument
/// past any leading `-c <key>=<value>` pairs.
fn subcommand_name(args: &[&std::ffi::OsStr]) -> String {
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        if arg.to_str() == Some("-c") {
            it.next();
        } else {
            return arg.to_string_lossy().into_owned();
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{
        add_submodule, allow_file_submodules, commit_file, commit_gitlink_bump, commit_super_index,
        init_repo, TempDir,
    };

    /// Fixture superproject with one submodule at `sub/` (child committed at
    /// `c.txt` = "sub one"). Returns `(superproject, child)`.
    fn submodule_fixture(tag: &str) -> (TempDir, TempDir) {
        let child = init_repo(&format!("{tag}-child"));
        commit_file(child.path(), "c.txt", "sub one\n");
        let superproject = init_repo(&format!("{tag}-super"));
        commit_file(superproject.path(), "a.txt", "one\n");
        add_submodule(superproject.path(), child.path(), "sub");
        (superproject, child)
    }

    /// Remove the submodule at `sub_rel` from the superproject history
    /// (gitlink + `.gitmodules`) and commit — how an upstream deletes a
    /// submodule, leaving downstream checkouts with an orphaned nested repo.
    fn commit_submodule_removal(super_path: &Path, sub_rel: &str) {
        let repo = Repository::open(super_path).unwrap();
        let mut index = repo.index().unwrap();
        index.remove_path(Path::new(sub_rel)).unwrap();
        index.remove_path(Path::new(".gitmodules")).unwrap();
        index.write().unwrap();
        commit_super_index(super_path, "remove submodule");
    }

    /// Remove ONE submodule at `sub_rel` (gitlink + its `.gitmodules` entry)
    /// and commit, keeping every other submodule registered — how an
    /// upstream deletes one of several submodules.
    fn commit_one_submodule_removal(super_path: &Path, sub_rel: &str) {
        let repo = Repository::open(super_path).unwrap();
        let mut index = repo.index().unwrap();
        index.remove_path(Path::new(sub_rel)).unwrap();
        {
            let mut cfg = git2::Config::open(&super_path.join(".gitmodules")).unwrap();
            cfg.remove(&format!("submodule.{sub_rel}.path")).unwrap();
            cfg.remove(&format!("submodule.{sub_rel}.url")).unwrap();
        }
        index.add_path(Path::new(".gitmodules")).unwrap();
        index.write().unwrap();
        commit_super_index(super_path, "remove one submodule");
    }

    /// Fixture with a nested submodule chain: superproject → `sub` (child)
    /// → `inner` (grandchild). Returns `(superproject, child, grandchild)`.
    fn nested_submodule_fixture(tag: &str) -> (TempDir, TempDir, TempDir) {
        let grandchild = init_repo(&format!("{tag}-grand"));
        commit_file(grandchild.path(), "g.txt", "deep one\n");
        let child = init_repo(&format!("{tag}-child"));
        commit_file(child.path(), "c.txt", "sub one\n");
        add_submodule(child.path(), grandchild.path(), "inner");
        let superproject = init_repo(&format!("{tag}-super"));
        commit_file(superproject.path(), "a.txt", "one\n");
        add_submodule(superproject.path(), child.path(), "sub");
        (superproject, child, grandchild)
    }

    /// Plant a fake (stale) module git dir with a file inside, as a removed
    /// submodule's leftover would look.
    fn plant_dead_module_dir(modules_dir: &Path, rel: &str) -> PathBuf {
        let dir = modules_dir.join(rel);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("HEAD"), "ref: refs/heads/main\n").unwrap();
        dir
    }

    /// Self-cleaning scratch dir for a cache root (testutil's TempDir is tied
    /// to `init_repo`; a cache root must start empty and non-git).
    struct CacheRoot(PathBuf);

    impl CacheRoot {
        fn new(tag: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "intent-git-repocache-{tag}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&path).unwrap();
            CacheRoot(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for CacheRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn file_url(path: &Path) -> String {
        format!("file://{}", path.display())
    }

    fn head_sha(path: &Path) -> String {
        Repository::open(path)
            .unwrap()
            .head()
            .unwrap()
            .target()
            .unwrap()
            .to_string()
    }

    /// Cache miss → a fresh clone lands at `<root>/<owner>/<repo>` with the
    /// origin's default branch checked out and its content on disk.
    #[tokio::test]
    async fn fresh_clone_creates_cache() {
        let origin = init_repo("repocache-origin-fresh");
        commit_file(origin.path(), "a.txt", "one\n");
        let root = CacheRoot::new("fresh");

        let path = ensure_cached_repo(
            root.path(),
            &file_url(origin.path()),
            "acme",
            "widget",
            None,
        )
        .await
        .unwrap();

        assert_eq!(path, root.path().join("acme").join("widget"));
        assert_eq!(
            std::fs::read_to_string(path.join("a.txt")).unwrap(),
            "one\n"
        );
        assert_eq!(head_sha(&path), head_sha(origin.path()));
        // The clone checked out the origin's default branch (not detached).
        let repo = Repository::open(&path).unwrap();
        assert!(repo.head().unwrap().is_branch());
    }

    /// Cache hit → refresh fetches the new origin commit and hard-resets to
    /// it, without re-cloning (a marker planted in `.git` survives).
    #[tokio::test]
    async fn refresh_updates_existing_cache_without_reclone() {
        let origin = init_repo("repocache-origin-refresh");
        commit_file(origin.path(), "a.txt", "one\n");
        let root = CacheRoot::new("refresh");
        let url = file_url(origin.path());

        let path = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        let marker = path.join(".git").join("intent-cache-marker");
        std::fs::write(&marker, "keep").unwrap();

        commit_file(origin.path(), "a.txt", "two\n");
        let path2 = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        assert_eq!(path, path2);
        assert!(marker.exists(), "refresh must not re-clone");
        assert_eq!(
            std::fs::read_to_string(path.join("a.txt")).unwrap(),
            "two\n"
        );
        assert_eq!(head_sha(&path), head_sha(origin.path()));
    }

    /// A cache that diverged from origin (local commit) is clobbered back to
    /// the origin tip by the refresh's hard reset — still no re-clone.
    #[tokio::test]
    async fn refresh_clobbers_diverged_cache() {
        let origin = init_repo("repocache-origin-diverge");
        commit_file(origin.path(), "a.txt", "one\n");
        let root = CacheRoot::new("diverge");
        let url = file_url(origin.path());

        let path = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        // Diverge the cache with a local commit origin never saw.
        {
            let repo = Repository::open(&path).unwrap();
            let mut cfg = repo.config().unwrap();
            cfg.set_str("user.name", "Test").unwrap();
            cfg.set_str("user.email", "test@example.com").unwrap();
        }
        commit_file(&path, "local.txt", "rogue\n");
        let marker = path.join(".git").join("intent-cache-marker");
        std::fs::write(&marker, "keep").unwrap();

        let path2 = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        assert_eq!(path, path2);
        assert!(marker.exists(), "divergence heals via reset, not re-clone");
        assert_eq!(head_sha(&path), head_sha(origin.path()));
        assert!(
            !path.join("local.txt").exists(),
            "hard reset must discard the rogue commit's file"
        );
    }

    /// A corrupt cache (gutted `.git`) self-heals: the dir is deleted and
    /// re-cloned instead of failing the flow.
    #[tokio::test]
    async fn corrupt_cache_self_heals_by_recloning() {
        let origin = init_repo("repocache-origin-corrupt");
        commit_file(origin.path(), "a.txt", "one\n");
        let root = CacheRoot::new("corrupt");
        let url = file_url(origin.path());

        let path = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        let marker = path.join(".git").join("intent-cache-marker");
        std::fs::write(&marker, "keep").unwrap();

        // Corrupt: replace .git with a garbage file.
        std::fs::remove_dir_all(path.join(".git")).unwrap();
        std::fs::write(path.join(".git"), "not a git dir").unwrap();

        let path2 = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        assert_eq!(path, path2);
        assert!(!marker.exists(), "self-heal must have re-cloned");
        assert_eq!(
            std::fs::read_to_string(path.join("a.txt")).unwrap(),
            "one\n"
        );
        assert_eq!(head_sha(&path), head_sha(origin.path()));
    }

    /// A cache dir that is not a repository at all (e.g. an interrupted prior
    /// clone left plain files) also self-heals into a fresh clone.
    #[tokio::test]
    async fn non_repo_cache_dir_self_heals() {
        let origin = init_repo("repocache-origin-nonrepo");
        commit_file(origin.path(), "a.txt", "one\n");
        let root = CacheRoot::new("nonrepo");
        let url = file_url(origin.path());

        let path = root.path().join("acme").join("widget");
        std::fs::create_dir_all(&path).unwrap();
        std::fs::write(path.join("junk.txt"), "leftover").unwrap();

        let path2 = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        assert_eq!(path, path2);
        assert!(!path.join("junk.txt").exists());
        assert_eq!(
            std::fs::read_to_string(path.join("a.txt")).unwrap(),
            "one\n"
        );
    }

    /// A clone failure (unreachable URL, no prior cache) surfaces as an error
    /// and leaves no half-written cache dir behind.
    #[tokio::test]
    async fn failed_clone_errors_and_leaves_no_cache() {
        let root = CacheRoot::new("clonefail");
        let missing = std::env::temp_dir().join("intent-git-repocache-definitely-missing");
        let err = ensure_cached_repo(root.path(), &file_url(&missing), "acme", "widget", None)
            .await
            .expect_err("clone from a missing path must fail");
        assert!(matches!(err, Error::Internal(_)));
        assert!(!root.path().join("acme").join("widget").exists());
    }

    /// Owner/repo segments that would escape the cache root or read as a git
    /// option are rejected before any filesystem or git work.
    #[tokio::test]
    async fn invalid_segments_are_rejected() {
        let root = CacheRoot::new("badseg");
        for (owner, repo) in [
            ("", "repo"),
            ("owner", ""),
            ("..", "repo"),
            ("owner", ".."),
            (".", "repo"),
            ("a/b", "repo"),
            ("owner", "a\\b"),
            ("-owner", "repo"),
            ("owner", "--upload-pack=x"),
        ] {
            let err = ensure_cached_repo(root.path(), "file:///nowhere", owner, repo, None)
                .await
                .expect_err("invalid segment must be rejected");
            assert!(matches!(err, Error::InvalidParams(_)), "{owner:?}/{repo:?}");
        }
    }

    /// The cache is keyed by owner/repo segments only, so a cached clone
    /// whose `origin` does not match the requested URL (same owner/repo pair
    /// on a different host/source) is wiped and re-cloned from the requested
    /// URL — never served as a hit with the wrong content.
    #[tokio::test]
    async fn origin_mismatch_recloned_from_requested_url() {
        let origin_a = init_repo("repocache-origin-hosta");
        commit_file(origin_a.path(), "a.txt", "host A\n");
        let origin_b = init_repo("repocache-origin-hostb");
        commit_file(origin_b.path(), "b.txt", "host B\n");
        let root = CacheRoot::new("hostkey");

        let path = ensure_cached_repo(
            root.path(),
            &file_url(origin_a.path()),
            "acme",
            "widget",
            None,
        )
        .await
        .unwrap();
        let marker = path.join(".git").join("intent-cache-marker");
        std::fs::write(&marker, "keep").unwrap();

        // Same owner/repo pair, different source URL: must re-clone from B.
        let path2 = ensure_cached_repo(
            root.path(),
            &file_url(origin_b.path()),
            "acme",
            "widget",
            None,
        )
        .await
        .unwrap();

        assert_eq!(path, path2);
        assert!(!marker.exists(), "origin mismatch must re-clone");
        assert!(!path.join("a.txt").exists(), "host A content gone");
        assert_eq!(
            std::fs::read_to_string(path.join("b.txt")).unwrap(),
            "host B\n"
        );
        assert_eq!(head_sha(&path), head_sha(origin_b.path()));
    }

    /// Untracked pollution in the cache work tree (e.g. leftovers from a
    /// process killed mid-checkout) is cleaned by the refresh, so it never
    /// copies into hydrated checkouts.
    #[tokio::test]
    async fn refresh_cleans_untracked_pollution() {
        let origin = init_repo("repocache-origin-clean");
        commit_file(origin.path(), "a.txt", "one\n");
        let root = CacheRoot::new("clean");
        let url = file_url(origin.path());

        let path = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        std::fs::write(path.join("pollution.txt"), "untracked leftover").unwrap();
        std::fs::create_dir_all(path.join("junk-dir")).unwrap();
        std::fs::write(path.join("junk-dir").join("x"), "y").unwrap();

        let path2 = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        assert_eq!(path, path2);
        assert!(
            !path.join("pollution.txt").exists(),
            "refresh must clean untracked files"
        );
        assert!(
            !path.join("junk-dir").exists(),
            "refresh must clean untracked directories"
        );
        assert_eq!(
            std::fs::read_to_string(path.join("a.txt")).unwrap(),
            "one\n"
        );
    }

    /// A remote that changed its default branch after the cache was cloned:
    /// the refresh re-resolves `origin/HEAD` (`remote set-head --auto`) and
    /// tracks the new default instead of pinning the obsolete one.
    #[tokio::test]
    async fn refresh_follows_changed_remote_default_branch() {
        let origin = init_repo("repocache-origin-sethead");
        commit_file(origin.path(), "a.txt", "one\n");
        let root = CacheRoot::new("sethead");
        let url = file_url(origin.path());

        let path = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        let marker = path.join(".git").join("intent-cache-marker");
        std::fs::write(&marker, "keep").unwrap();

        // Flip the remote's default branch to a new branch with new content.
        {
            let repo = Repository::open(origin.path()).unwrap();
            let head = repo.head().unwrap().peel_to_commit().unwrap();
            repo.branch("develop", &head, false).unwrap();
            repo.set_head("refs/heads/develop").unwrap();
        }
        commit_file(origin.path(), "dev.txt", "on develop\n");

        let path2 = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        assert_eq!(path, path2);
        assert!(
            marker.exists(),
            "default-branch change refreshes, no re-clone"
        );
        let repo = Repository::open(&path).unwrap();
        assert_eq!(
            repo.head().unwrap().shorthand().unwrap(),
            "develop",
            "cache tracks the remote's new default branch"
        );
        assert_eq!(
            std::fs::read_to_string(path.join("dev.txt")).unwrap(),
            "on develop\n"
        );
    }

    /// Concurrent callers for the same repo serialize on the per-repo lock:
    /// while the lock is held externally, `ensure_cached_repo` makes no
    /// progress; once released it completes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_ensures_serialize_on_the_repo_lock() {
        let origin = init_repo("repocache-origin-lock");
        commit_file(origin.path(), "a.txt", "one\n");
        let root = CacheRoot::new("lock");
        let url = file_url(origin.path());
        let cache_path = root.path().join("acme").join("widget");

        let lock = lock_for(&cache_path);
        let guard = lock.lock().await;

        let root_path = root.path().to_path_buf();
        let url2 = url.clone();
        let task = tokio::spawn(async move {
            ensure_cached_repo(&root_path, &url2, "acme", "widget", None).await
        });

        // While the lock is held the clone must not have started.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            !cache_path.exists(),
            "ensure must block on the per-repo lock"
        );

        drop(guard);
        let path = task.await.unwrap().unwrap();
        assert_eq!(path, cache_path);
        assert!(path.join("a.txt").exists());
    }

    /// `provision_direct_checkout` produces a standalone plain clone of the
    /// cache on the workspace branch, with `origin` retargeted at the real
    /// URL (never the cache path).
    #[tokio::test]
    async fn direct_checkout_is_standalone_on_branch_with_retargeted_origin() {
        let origin = init_repo("repocache-direct-basic");
        commit_file(origin.path(), "a.txt", "one\n");
        let root = CacheRoot::new("direct-basic");
        let url = file_url(origin.path());
        let cache = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        let checkout_root = CacheRoot::new("direct-basic-dst");
        let checkout = checkout_root.path().join("ws").join("widget");
        let sha = provision_direct_checkout(&cache, &checkout, &url, "ws-branch", None).unwrap();

        assert_eq!(sha, head_sha(origin.path()));
        let repo = Repository::open(&checkout).unwrap();
        assert!(!repo.is_worktree(), "direct checkout is a standalone repo");
        let head = repo.head().unwrap();
        assert_eq!(head.shorthand().unwrap(), "ws-branch");
        assert_eq!(
            repo.find_remote("origin").unwrap().url().unwrap(),
            url.as_str(),
            "origin retargeted from the cache to the real URL"
        );
        assert_eq!(
            std::fs::read_to_string(checkout.join("a.txt")).unwrap(),
            "one\n"
        );
    }

    /// A `base_ref` naming a non-default origin branch resolves through the
    /// remote-tracking refs copied from the cache.
    #[tokio::test]
    async fn direct_checkout_resolves_non_default_base_ref() {
        let origin = init_repo("repocache-direct-base");
        commit_file(origin.path(), "a.txt", "one\n");
        // Pin `base` at the first commit, then advance the default branch.
        let base_sha = head_sha(origin.path());
        crate::testutil::create_branch(origin.path(), "base");
        commit_file(origin.path(), "a.txt", "two\n");
        let root = CacheRoot::new("direct-base");
        let url = file_url(origin.path());
        let cache = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        let checkout_root = CacheRoot::new("direct-base-dst");
        let checkout = checkout_root.path().join("ws").join("widget");
        let sha =
            provision_direct_checkout(&cache, &checkout, &url, "ws-branch", Some("base")).unwrap();

        assert_eq!(sha, base_sha, "branch starts at the base ref");
        assert_eq!(
            std::fs::read_to_string(checkout.join("a.txt")).unwrap(),
            "one\n",
            "tracked files match the base ref, not the default tip"
        );
    }

    /// An unresolvable `base_ref` surfaces as the typed error and removes the
    /// partially provisioned checkout.
    #[tokio::test]
    async fn direct_checkout_rejects_unresolvable_base_ref_and_cleans_up() {
        let origin = init_repo("repocache-direct-badref");
        commit_file(origin.path(), "a.txt", "one\n");
        let root = CacheRoot::new("direct-badref");
        let url = file_url(origin.path());
        let cache = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        let checkout_root = CacheRoot::new("direct-badref-dst");
        let checkout = checkout_root.path().join("ws").join("widget");
        let err = provision_direct_checkout(&cache, &checkout, &url, "ws-branch", Some("nope"))
            .unwrap_err();
        assert!(
            matches!(err, Error::BaseRefUnresolvable { ref base_ref } if base_ref == "nope"),
            "got: {err:?}"
        );
        assert!(!checkout.exists(), "partial checkout is removed on failure");
    }

    /// Several concurrent ensures for the same repo all succeed and agree on
    /// the final state (the lock serializes the refresh/clone work).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parallel_ensures_all_succeed() {
        let origin = init_repo("repocache-origin-parallel");
        commit_file(origin.path(), "a.txt", "one\n");
        let root = CacheRoot::new("parallel");
        let url = file_url(origin.path());

        let mut tasks = Vec::new();
        for _ in 0..4 {
            let root_path = root.path().to_path_buf();
            let url = url.clone();
            tasks.push(tokio::spawn(async move {
                ensure_cached_repo(&root_path, &url, "acme", "widget", None).await
            }));
        }
        for task in tasks {
            let path = task.await.unwrap().unwrap();
            assert_eq!(head_sha(&path), head_sha(origin.path()));
        }
    }

    /// Cache miss on a repo with submodules → the recursive clone leaves the
    /// cache with a populated submodule work tree at the recorded gitlink.
    #[tokio::test]
    async fn fresh_clone_populates_submodule_work_trees() {
        allow_file_submodules();
        let (origin, child) = submodule_fixture("repocache-subfresh");
        let root = CacheRoot::new("subfresh");

        let path = ensure_cached_repo(
            root.path(),
            &file_url(origin.path()),
            "acme",
            "widget",
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            std::fs::read_to_string(path.join("sub").join("c.txt")).unwrap(),
            "sub one\n",
            "submodule work tree is populated"
        );
        assert_eq!(
            head_sha(&path.join("sub")),
            head_sha(child.path()),
            "submodule sits at the recorded gitlink"
        );
    }

    /// Upstream bumps a submodule gitlink → the refresh moves the cache's
    /// submodule work tree to the new commit, without re-cloning the cache.
    #[tokio::test]
    async fn refresh_moves_submodule_to_bumped_gitlink() {
        allow_file_submodules();
        let (origin, child) = submodule_fixture("repocache-subbump");
        let root = CacheRoot::new("subbump");
        let url = file_url(origin.path());

        let path = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        let marker = path.join(".git").join("intent-cache-marker");
        std::fs::write(&marker, "keep").unwrap();

        commit_file(child.path(), "c.txt", "sub two\n");
        let new_sha = head_sha(child.path());
        commit_gitlink_bump(origin.path(), "sub", &new_sha);

        let path2 = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        assert_eq!(path, path2);
        assert!(marker.exists(), "gitlink bump refreshes, no re-clone");
        assert_eq!(
            head_sha(&path.join("sub")),
            new_sha,
            "submodule moved to the new gitlink"
        );
        assert_eq!(
            std::fs::read_to_string(path.join("sub").join("c.txt")).unwrap(),
            "sub two\n"
        );
    }

    /// Untracked pollution inside a live submodule's work tree is invisible
    /// to the superproject clean; the per-submodule clean removes it.
    #[tokio::test]
    async fn refresh_cleans_pollution_inside_submodule() {
        allow_file_submodules();
        let (origin, _child) = submodule_fixture("repocache-subclean");
        let root = CacheRoot::new("subclean");
        let url = file_url(origin.path());

        let path = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        std::fs::write(path.join("sub").join("pollution.txt"), "leftover").unwrap();

        let path2 = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        assert_eq!(path, path2);
        assert!(
            !path.join("sub").join("pollution.txt").exists(),
            "refresh must clean inside submodule work trees"
        );
        assert_eq!(
            std::fs::read_to_string(path.join("sub").join("c.txt")).unwrap(),
            "sub one\n",
            "tracked submodule content survives"
        );
    }

    /// Upstream removed the submodule entirely: after the refresh's reset the
    /// old checkout is an orphaned untracked nested repo, which the double-`f`
    /// clean removes — still no re-clone.
    #[tokio::test]
    async fn refresh_removes_orphaned_submodule_checkout() {
        allow_file_submodules();
        let (origin, _child) = submodule_fixture("repocache-suborphan");
        let root = CacheRoot::new("suborphan");
        let url = file_url(origin.path());

        let path = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        assert!(path.join("sub").join("c.txt").exists());
        let marker = path.join(".git").join("intent-cache-marker");
        std::fs::write(&marker, "keep").unwrap();

        commit_submodule_removal(origin.path(), "sub");

        let path2 = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        assert_eq!(path, path2);
        assert!(marker.exists(), "submodule removal refreshes, no re-clone");
        assert!(
            !path.join("sub").exists(),
            "orphaned submodule checkout is removed"
        );
        assert!(!path.join(".gitmodules").exists());
    }

    /// A refresh anomaly involving a submodule (here: its gitfile replaced
    /// with garbage) self-heals via wipe + re-clone and never errors out of
    /// `ensure_cached_repo`.
    #[tokio::test]
    async fn refresh_submodule_anomaly_self_heals_by_recloning() {
        allow_file_submodules();
        let (origin, child) = submodule_fixture("repocache-subheal");
        let root = CacheRoot::new("subheal");
        let url = file_url(origin.path());

        let path = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        let marker = path.join(".git").join("intent-cache-marker");
        std::fs::write(&marker, "keep").unwrap();

        // Break the submodule: its `.git` gitfile becomes garbage, so the
        // refresh's submodule steps fail.
        let gitfile = path.join("sub").join(".git");
        std::fs::remove_file(&gitfile).unwrap();
        std::fs::write(&gitfile, "not a gitfile").unwrap();

        let path2 = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        assert_eq!(path, path2);
        assert!(!marker.exists(), "anomaly must self-heal via re-clone");
        assert_eq!(
            std::fs::read_to_string(path.join("sub").join("c.txt")).unwrap(),
            "sub one\n"
        );
        assert_eq!(head_sha(&path.join("sub")), head_sha(child.path()));
    }

    /// Every value in the given repo config file, so a test can assert no
    /// value still references the cache path.
    fn config_values(config_path: &Path) -> Vec<String> {
        let cfg = git2::Config::open(config_path).unwrap();
        let mut values = Vec::new();
        let mut entries = cfg.entries(None).unwrap();
        while let Some(entry) = entries.next() {
            let entry = entry.unwrap();
            if let Ok(value) = entry.value() {
                values.push(value.to_string());
            }
        }
        values
    }

    /// Direct hydration of a repo with a submodule populates the submodule
    /// work tree from the cache's local module git dirs — no network: the
    /// child repo (the only remote the submodule could fetch from) is deleted
    /// before provisioning.
    #[tokio::test]
    async fn direct_checkout_populates_submodules_from_cache_without_network() {
        allow_file_submodules();
        let (origin, child) = submodule_fixture("repocache-directsub");
        let root = CacheRoot::new("directsub");
        let url = file_url(origin.path());
        let cache = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        let sub_sha = head_sha(child.path());
        let child_url = child.path().to_string_lossy().to_string();
        // Any fetch/clone from the submodule's real URL now fails: population
        // must come from the cache's local module git dirs alone.
        drop(child);

        let checkout_root = CacheRoot::new("directsub-dst");
        let checkout = checkout_root.path().join("ws").join("widget");
        provision_direct_checkout(&cache, &checkout, &url, "ws-branch", None).unwrap();

        assert_eq!(
            std::fs::read_to_string(checkout.join("sub").join("c.txt")).unwrap(),
            "sub one\n",
            "submodule work tree is populated"
        );
        assert_eq!(head_sha(&checkout.join("sub")), sub_sha);
        // Final submodule URLs point at the real `.gitmodules` remote, not
        // the cache: the recorded config for the superproject and the module
        // both name the child URL, and no config value names the cache path.
        let repo = Repository::open(&checkout).unwrap();
        let cfg = repo.config().unwrap();
        assert_eq!(cfg.get_string("submodule.sub.url").unwrap(), child_url);
        let cache_str = cache.display().to_string();
        for config in [
            checkout.join(".git").join("config"),
            checkout
                .join(".git")
                .join("modules")
                .join("sub")
                .join("config"),
        ] {
            for value in config_values(&config) {
                assert!(
                    !value.contains(&cache_str),
                    "config value {value:?} still references the cache path"
                );
            }
        }
        // The cache itself is untouched by the hydration.
        assert_eq!(
            std::fs::read_to_string(cache.join("sub").join("c.txt")).unwrap(),
            "sub one\n"
        );
    }

    /// The cache must remain safe to delete: after direct hydration, wiping
    /// the cache leaves a fully functional checkout, submodule included.
    #[tokio::test]
    async fn direct_checkout_survives_cache_deletion() {
        allow_file_submodules();
        let (origin, child) = submodule_fixture("repocache-directrm");
        let root = CacheRoot::new("directrm");
        let url = file_url(origin.path());
        let cache = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        let checkout_root = CacheRoot::new("directrm-dst");
        let checkout = checkout_root.path().join("ws").join("widget");
        provision_direct_checkout(&cache, &checkout, &url, "ws-branch", None).unwrap();

        std::fs::remove_dir_all(&cache).unwrap();

        let repo = Repository::open(&checkout).unwrap();
        assert_eq!(repo.head().unwrap().shorthand().unwrap(), "ws-branch");
        let sub = Repository::open(checkout.join("sub")).unwrap();
        assert_eq!(
            sub.head().unwrap().target().unwrap().to_string(),
            head_sha(child.path())
        );
        // A force-resync still works from the checkout's own objects.
        update_checkout_submodules(&checkout).unwrap();
        assert_eq!(
            std::fs::read_to_string(checkout.join("sub").join("c.txt")).unwrap(),
            "sub one\n"
        );
    }

    /// A gitlink the local module git dirs do not hold is never fetched from
    /// the real remote: the strictly-offline update errors — callers degrade
    /// with a warning — even though the recorded URL is alive and reachable
    /// (the child repo, over the test-allowed `file` protocol).
    #[tokio::test]
    async fn update_checkout_submodules_never_touches_the_network() {
        allow_file_submodules();
        let (origin, child) = submodule_fixture("repocache-nofetch");
        let root = CacheRoot::new("nofetch");
        let url = file_url(origin.path());
        let cache = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        let checkout_root = CacheRoot::new("nofetch-dst");
        let checkout = checkout_root.path().join("ws").join("widget");
        provision_direct_checkout(&cache, &checkout, &url, "ws-branch", None).unwrap();

        // Advance the child AFTER hydration and bump the checkout's gitlink
        // to the new commit: the checkout's module git dir lacks the object,
        // but the live child repo could serve it over a fetch — the update
        // must fail instead of asking.
        commit_file(child.path(), "c.txt", "sub two\n");
        let new_sub_sha = head_sha(child.path());
        commit_gitlink_bump(&checkout, "sub", &new_sub_sha);

        update_checkout_submodules(&checkout)
            .expect_err("update must fail offline instead of fetching the missing gitlink");
        assert_eq!(
            std::fs::read_to_string(checkout.join("sub").join("c.txt")).unwrap(),
            "sub one\n",
            "submodule work tree untouched — nothing was fetched from the network"
        );
    }

    /// A `base_ref` pinned at an older gitlink: the checkout's submodule
    /// lands on that older commit, resolved from the cache's local module
    /// objects (the cache tip has moved past it).
    #[tokio::test]
    async fn direct_checkout_syncs_submodule_to_base_ref_gitlink() {
        allow_file_submodules();
        let (origin, child) = submodule_fixture("repocache-directbase");
        let old_sub_sha = head_sha(child.path());
        // Pin `base` at the old gitlink, then bump the pin on the default tip.
        crate::testutil::create_branch(origin.path(), "base");
        commit_file(child.path(), "c.txt", "sub two\n");
        let new_sub_sha = head_sha(child.path());
        commit_gitlink_bump(origin.path(), "sub", &new_sub_sha);

        let root = CacheRoot::new("directbase");
        let url = file_url(origin.path());
        let cache = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        assert_eq!(head_sha(&cache.join("sub")), new_sub_sha, "cache at tip");

        let checkout_root = CacheRoot::new("directbase-dst");
        let checkout = checkout_root.path().join("ws").join("widget");
        provision_direct_checkout(&cache, &checkout, &url, "ws-branch", Some("base")).unwrap();

        assert_eq!(
            head_sha(&checkout.join("sub")),
            old_sub_sha,
            "submodule sits at the base ref's gitlink, not the cache tip"
        );
        assert_eq!(
            std::fs::read_to_string(checkout.join("sub").join("c.txt")).unwrap(),
            "sub one\n"
        );
    }

    /// A submodule anomaly during direct hydration (no local module git dirs
    /// and an unreachable real URL) degrades to an unpopulated submodule with
    /// a warning — provisioning still succeeds.
    #[tokio::test]
    async fn direct_checkout_submodule_failure_degrades_gracefully() {
        allow_file_submodules();
        let (origin, child) = submodule_fixture("repocache-directdeg");
        let root = CacheRoot::new("directdeg");
        let url = file_url(origin.path());
        let cache = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        // No local module git dirs to copy AND no reachable real URL: the
        // submodule update has nothing to populate from.
        std::fs::remove_dir_all(cache.join(".git").join("modules")).unwrap();
        drop(child);

        let checkout_root = CacheRoot::new("directdeg-dst");
        let checkout = checkout_root.path().join("ws").join("widget");
        let sha = provision_direct_checkout(&cache, &checkout, &url, "ws-branch", None).unwrap();

        assert!(!sha.is_empty(), "provisioning succeeds despite the anomaly");
        let repo = Repository::open(&checkout).unwrap();
        assert_eq!(repo.head().unwrap().shorthand().unwrap(), "ws-branch");
        assert_eq!(
            std::fs::read_to_string(checkout.join("a.txt")).unwrap(),
            "one\n",
            "superproject content is intact"
        );
        assert!(
            !checkout.join("sub").join("c.txt").exists(),
            "submodule is left unpopulated"
        );
    }

    /// Upstream removes one of two submodules: the refresh prunes the dead
    /// module's `.git/modules` dir from the cache while the surviving
    /// module's dir stays intact and functional — no re-clone.
    #[tokio::test]
    async fn refresh_prunes_dead_module_dir_keeps_live_one() {
        allow_file_submodules();
        let (origin, _child) = submodule_fixture("repocache-prune");
        let doomed = init_repo("repocache-prune-doomed");
        commit_file(doomed.path(), "d.txt", "doomed one\n");
        add_submodule(origin.path(), doomed.path(), "doomedsub");
        let root = CacheRoot::new("prune");
        let url = file_url(origin.path());

        let path = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        let modules = path.join(".git").join("modules");
        assert!(modules.join("sub").is_dir());
        assert!(modules.join("doomedsub").is_dir());
        let marker = path.join(".git").join("intent-cache-marker");
        std::fs::write(&marker, "keep").unwrap();

        commit_one_submodule_removal(origin.path(), "doomedsub");

        let path2 = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        assert_eq!(path, path2);
        assert!(marker.exists(), "prune happens in-place, no re-clone");
        assert!(
            !modules.join("doomedsub").exists(),
            "dead module git dir is pruned"
        );
        assert!(
            modules.join("sub").is_dir(),
            "live module git dir survives the prune"
        );
        assert_eq!(
            std::fs::read_to_string(path.join("sub").join("c.txt")).unwrap(),
            "sub one\n",
            "live submodule work tree still functional"
        );
        // A subsequent refresh with the pruned cache still succeeds in-place.
        let path3 = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        assert_eq!(path, path3);
        assert!(marker.exists(), "pruned cache refreshes cleanly");
    }

    /// A live nested submodule's module dir (`.git/modules/sub/modules/inner`)
    /// survives the prune, while a planted dead dir at both nesting levels is
    /// removed.
    #[tokio::test]
    async fn refresh_prune_preserves_live_nested_module() {
        allow_file_submodules();
        let (origin, _child, grandchild) = nested_submodule_fixture("repocache-prunenest");
        let root = CacheRoot::new("prunenest");
        let url = file_url(origin.path());

        let path = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        let modules = path.join(".git").join("modules");
        let inner_module = modules.join("sub").join("modules").join("inner");
        assert!(inner_module.is_dir(), "nested module dir exists");
        let dead_top = plant_dead_module_dir(&modules, "deadtop");
        let dead_nested = plant_dead_module_dir(&modules.join("sub").join("modules"), "deadinner");
        let marker = path.join(".git").join("intent-cache-marker");
        std::fs::write(&marker, "keep").unwrap();

        let path2 = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();

        assert_eq!(path, path2);
        assert!(marker.exists(), "prune happens in-place, no re-clone");
        assert!(!dead_top.exists(), "dead top-level module dir is pruned");
        assert!(!dead_nested.exists(), "dead nested module dir is pruned");
        assert!(inner_module.is_dir(), "live nested module dir survives");
        assert_eq!(
            std::fs::read_to_string(path.join("sub").join("inner").join("g.txt")).unwrap(),
            "deep one\n",
            "nested submodule work tree still functional"
        );
        assert_eq!(
            head_sha(&path.join("sub").join("inner")),
            head_sha(grandchild.path())
        );
    }

    /// Direct hydration from a cache still carrying dead module dirs (a cache
    /// refreshed before pruning existed): the checkout's `.git/modules` gets
    /// only the live modules, at both nesting levels, and the live ones stay
    /// functional.
    #[tokio::test]
    async fn direct_checkout_filters_dead_module_dirs() {
        allow_file_submodules();
        let (origin, _child, grandchild) = nested_submodule_fixture("repocache-hydratefilter");
        let root = CacheRoot::new("hydratefilter");
        let url = file_url(origin.path());
        let cache = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        let cache_modules = cache.join(".git").join("modules");
        plant_dead_module_dir(&cache_modules, "deadtop");
        plant_dead_module_dir(&cache_modules.join("sub").join("modules"), "deadinner");

        let checkout_root = CacheRoot::new("hydratefilter-dst");
        let checkout = checkout_root.path().join("ws").join("widget");
        provision_direct_checkout(&cache, &checkout, &url, "ws-branch", None).unwrap();

        let modules = checkout.join(".git").join("modules");
        assert!(
            !modules.join("deadtop").exists(),
            "dead top-level module dir is not copied into the checkout"
        );
        assert!(
            !modules
                .join("sub")
                .join("modules")
                .join("deadinner")
                .exists(),
            "dead nested module dir is not copied into the checkout"
        );
        assert!(modules.join("sub").is_dir(), "live module dir is copied");
        assert!(
            modules.join("sub").join("modules").join("inner").is_dir(),
            "live nested module dir is copied"
        );
        assert_eq!(
            std::fs::read_to_string(checkout.join("sub").join("c.txt")).unwrap(),
            "sub one\n",
            "live submodule work tree is populated"
        );
        assert_eq!(
            std::fs::read_to_string(checkout.join("sub").join("inner").join("g.txt")).unwrap(),
            "deep one\n",
            "live nested submodule work tree is populated"
        );
        assert_eq!(
            head_sha(&checkout.join("sub").join("inner")),
            head_sha(grandchild.path())
        );
        // The cache keeps its dead dirs — hydration filters, never mutates
        // the cache.
        assert!(cache_modules.join("deadtop").is_dir());
    }

    /// A `base_ref` older than the cache tip can select a `sub` gitlink
    /// whose own `.gitmodules` still names a nested module the tip dropped.
    /// The hydration copy must not judge nested liveness from the tip's
    /// work trees: a nested module gitdir the cache still carries is copied
    /// wholesale and the offline update populates it (regression: the
    /// nested filter read the tip's `.gitmodules`, dropping the
    /// live-at-base gitdir and leaving the nested submodule unpopulated).
    #[tokio::test]
    async fn direct_checkout_populates_nested_submodule_dropped_at_tip() {
        allow_file_submodules();
        let (origin, child, grandchild) = nested_submodule_fixture("repocache-nestedbase");
        let root = CacheRoot::new("nestedbase");
        let url = file_url(origin.path());
        // Pin `base` while the nested chain is fully live, and seed the
        // cache there so it holds the nested module gitdir.
        crate::testutil::create_branch(origin.path(), "base");
        let cache = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        let inner_gitdir = cache
            .join(".git")
            .join("modules")
            .join("sub")
            .join("modules")
            .join("inner");
        assert!(
            inner_gitdir.is_dir(),
            "seeded cache holds the nested gitdir"
        );

        // Upstream drops the nested module and the superproject follows:
        // the tip no longer names `inner` anywhere.
        commit_one_submodule_removal(child.path(), "inner");
        let new_sub_sha = head_sha(child.path());
        commit_gitlink_bump(origin.path(), "sub", &new_sub_sha);

        // Refresh the cache to the new tip, restoring the nested gitdir the
        // refresh prune drops — the shape of a cache refreshed before
        // pruning existed, still carrying a history-only module.
        let saved = CacheRoot::new("nestedbase-saved");
        let saved_inner = saved.path().join("inner");
        copy_dir_recursive(&inner_gitdir, &saved_inner).unwrap();
        let cache = ensure_cached_repo(root.path(), &url, "acme", "widget", None)
            .await
            .unwrap();
        assert!(
            !inner_gitdir.exists(),
            "refresh prunes the tip-dead nested gitdir"
        );
        copy_dir_recursive(&saved_inner, &inner_gitdir).unwrap();

        let inner_sha = head_sha(grandchild.path());
        // No network: the nested module's only remote goes away, so its
        // population must come from the copied gitdir alone.
        drop(grandchild);
        drop(child);

        let checkout_root = CacheRoot::new("nestedbase-dst");
        let checkout = checkout_root.path().join("ws").join("widget");
        provision_direct_checkout(&cache, &checkout, &url, "ws-branch", Some("base")).unwrap();

        assert_eq!(
            std::fs::read_to_string(checkout.join("sub").join("inner").join("g.txt")).unwrap(),
            "deep one\n",
            "nested submodule work tree is populated at the base ref"
        );
        assert_eq!(head_sha(&checkout.join("sub").join("inner")), inner_sha);
        assert!(
            checkout
                .join(".git")
                .join("modules")
                .join("sub")
                .join("modules")
                .join("inner")
                .is_dir(),
            "nested module gitdir is copied despite being dead at the tip"
        );
    }
}
