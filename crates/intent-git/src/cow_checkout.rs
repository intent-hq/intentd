//! Standalone CoW checkout provisioning for `workspace.create` (§5.1).
//!
//! [`provision_cow_checkout`] is the copy-on-write counterpart of
//! [`crate::worktree::provision_worktree`]: instead of a linked worktree it
//! `cow_clone`s the whole repository directory (deps/build artifacts included),
//! then inside the clone creates + checks out the workspace branch from
//! `base_ref` and hard-resets tracked files to that base. Untracked files are
//! deliberately preserved — carrying `node_modules`/`target`-style artifacts
//! into the checkout for free is the point of CoW.
//!
//! [`provision_local_clone_checkout`] is the non-CoW standalone sibling: a
//! plain local clone of an arbitrary source checkout, used when the filesystem
//! cannot CoW-clone.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use git2::{BranchType, Repository};
use intent_core::{Error, Result};

use crate::cow::cow_clone_with_excludes;
use crate::map_git_err;
use crate::repo_cache::{provision_plain_clone_checkout, OriginTarget};

/// How many of the slowest fast-path subtree clones to name in the
/// provisioning summary log.
const SLOWEST_SUBTREES_LOGGED: usize = 5;

/// Phase timings and clone statistics for one CoW checkout provisioning, so a
/// slow `workspace.create`/`workspace.duplicate` is attributable from logs.
#[derive(Debug, Default)]
pub(crate) struct CowProvisionTimings {
    /// Wall-clock duration of the whole provisioning call.
    pub total: Duration,
    /// Duration of the CoW clone itself.
    pub cow_clone: Duration,
    /// The clone was a single whole-tree primitive call (macOS fast path).
    pub whole_tree_clone: bool,
    /// Slowest fast-path subtree clones, up to [`SLOWEST_SUBTREES_LOGGED`],
    /// sorted slowest-first (root-relative path, duration). Empty for
    /// whole-tree clones and walks without a subtree fast path.
    pub slowest_subtrees: Vec<(PathBuf, Duration)>,
    /// Directories skipped because they matched a `cowCloneExclude` entry.
    pub skipped_excluded: u64,
    /// Duration of stripping `.git/worktrees` registrations from the clone.
    pub strip_registrations: Duration,
    /// Duration of branch creation + checkout + hard reset in the clone.
    pub checkout: Duration,
    /// Duration of the post-reset submodule work: force-update, orphaned
    /// work-tree cleanup, and closing URL sync (zero when neither the
    /// source tip nor the checked-out ref involves submodules).
    pub submodule_update: Duration,
}

impl CowProvisionTimings {
    /// Human-readable `path=12ms, path2=3ms` list for the summary log.
    fn slowest_subtrees_display(&self) -> String {
        self.slowest_subtrees
            .iter()
            .map(|(p, d)| format!("{}={}ms", p.display(), d.as_millis()))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Provision a standalone CoW checkout: clone `repo_path` to `checkout_path`
/// with copy-on-write, then in the clone create `branch` from `base_ref`
/// (resolution order matches `provision_worktree`:
/// `refs/remotes/{remote}/{base_ref}` → `refs/heads/{base_ref}` → any
/// rev-parsable spec; no `base_ref` means HEAD), check it out, and hard-reset
/// tracked files to that base while preserving untracked files. An existing
/// branch of the same name is reused rather than recreated. Returns the SHA of
/// the commit the checkout lands on. On failure after the clone, the partially
/// provisioned `checkout_path` is removed best-effort.
///
/// When the checkout has `.gitmodules`, submodule work trees are force-synced
/// to their recorded gitlinks after the reset (`git submodule update --init
/// --recursive --force`): the hard reset moves the superproject alone, so a
/// `base_ref` whose gitlinks differ from the source tip would otherwise leave
/// the byte-copied submodule work trees at the wrong commit. The clone
/// carries the source's `.git/modules` git dirs, so the update resolves from
/// local objects — no network when the source (e.g. a fresh repo cache)
/// already holds the gitlink commits. Submodule work trees copied from the
/// source tip that the checked-out ref no longer registers (a `base_ref`
/// predating the submodule, or one where it was removed) are orphans —
/// nested repositories the hard reset and a plain clean both skip — so they
/// are removed along with their dead `.git/modules` entries
/// ([`remove_orphaned_submodules`]). A closing `git submodule sync
/// --recursive` then re-points `submodule.<name>.url` config at what
/// `.gitmodules` says (parity with the direct hydration path). Every
/// submodule step degrades to a warning on failure; none fails provisioning.
///
/// The clone inherits the source's `origin` remote verbatim (fetch URL and
/// any `remote.origin.pushurl` alike), so both are resolved the same way as
/// in [`provision_local_clone_checkout`] ([`resolve_source_origin`]): a
/// relative local path is absolutized against the SOURCE repository so it
/// still names the same upstream, a local path resolving to the source
/// checkout itself is removed, and network URLs / already-absolute local
/// paths carry over verbatim. A source with no `origin` inherits nothing, so
/// nothing is fixed up.
///
/// A `repo_path` that is itself a linked git worktree (its `.git` is a
/// gitfile) is refused with `Error::Unsupported`: the cloned gitfile would
/// still point into the ORIGINAL repository's `.git/worktrees/<name>`, so the
/// branch switch + hard reset below would rewrite the user's source checkout.
/// Callers route such repos to linked-worktree provisioning instead.
pub fn provision_cow_checkout(
    repo_path: &Path,
    checkout_path: &Path,
    branch: &str,
    base_ref: Option<&str>,
    remote: &str,
    clone_excludes: &[String],
) -> Result<String> {
    let (sha, timings) = provision_cow_checkout_timed(
        repo_path,
        checkout_path,
        branch,
        base_ref,
        remote,
        clone_excludes,
    )?;
    tracing::info!(
        checkout = %checkout_path.display(),
        total_ms = timings.total.as_millis() as u64,
        cow_clone_ms = timings.cow_clone.as_millis() as u64,
        whole_tree_clone = timings.whole_tree_clone,
        slowest_subtrees = %timings.slowest_subtrees_display(),
        skipped_excluded = timings.skipped_excluded,
        strip_registrations_ms = timings.strip_registrations.as_millis() as u64,
        checkout_ms = timings.checkout.as_millis() as u64,
        submodule_update_ms = timings.submodule_update.as_millis() as u64,
        "provision_cow_checkout: provisioning phase timings"
    );
    Ok(sha)
}

/// [`provision_cow_checkout`] returning the phase timings alongside the SHA
/// (the public entry point logs them; tests assert on them directly).
pub(crate) fn provision_cow_checkout_timed(
    repo_path: &Path,
    checkout_path: &Path,
    branch: &str,
    base_ref: Option<&str>,
    remote: &str,
    clone_excludes: &[String],
) -> Result<(String, CowProvisionTimings)> {
    let started = Instant::now();
    let mut timings = CowProvisionTimings::default();
    // Canonicalize first: the whole-tree clonefile(2) follows a symlink
    // root (materializing a real cloned tree), but the best-effort walk
    // preserves links without following the root — a symlinked `repo_path`
    // taking that path would be cloned as the symlink itself, leaving
    // `checkout_path` resolving THROUGH the link into the original repo —
    // so the registration strip and hard reset below would operate on the
    // user's source checkout.
    let repo_path = repo_path.canonicalize().map_err(|e| {
        Error::InvalidInput(format!(
            "cannot canonicalize repository path {}: {e}",
            repo_path.display()
        ))
    })?;
    let repo_path = repo_path.as_path();
    if repo_path.join(".git").is_file() {
        return Err(Error::Unsupported(format!(
            "repository at {} has a gitfile .git (linked worktree or submodule checkout); CoW-cloning it would corrupt the source checkout",
            repo_path.display()
        )));
    }
    if let Some(parent) = checkout_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| Error::Internal(format!("cannot create checkout parent dir: {e}")))?;
    }

    let clone_started = Instant::now();
    let clone_stats = cow_clone_with_excludes(repo_path, checkout_path, clone_excludes)?;
    timings.cow_clone = clone_started.elapsed();
    timings.whole_tree_clone = clone_stats.whole_tree;
    timings.skipped_excluded = clone_stats.skipped_excluded;
    let mut subtrees = clone_stats.subtree_timings;
    subtrees.sort_by_key(|entry| std::cmp::Reverse(entry.1));
    subtrees.truncate(SLOWEST_SUBTREES_LOGGED);
    timings.slowest_subtrees = subtrees;

    let result = (|| {
        let strip_started = Instant::now();
        strip_worktree_registrations(checkout_path)?;
        timings.strip_registrations = strip_started.elapsed();
        // Submodule paths as byte-copied from the source tip — nested ones
        // included, since a reset can orphan `sub/inner` while `sub` itself
        // survives — captured before the branch switch/hard reset below
        // moves HEAD: any of them the checked-out state no longer registers
        // is an orphaned work tree to remove after the reset. Gated on a
        // cheap probe so no-submodule repos never pay the read; unreadable
        // state degrades to no orphan cleanup.
        let pre_reset_submodules = if crate::submodule::has_submodules(checkout_path)
            || checkout_path.join(".git").join("modules").is_dir()
        {
            crate::submodule::recursive_submodule_paths(checkout_path)
                .unwrap_or_else(|e| {
                    tracing::warn!(
                        checkout = %checkout_path.display(),
                        error = %e,
                        "provision_cow_checkout: cannot read pre-reset submodule paths; skipping orphan cleanup"
                    );
                    std::collections::BTreeSet::default()
                })
        } else {
            std::collections::BTreeSet::default()
        };
        let checkout_started = Instant::now();
        let sha = checkout_in_clone(checkout_path, branch, base_ref, remote)?;
        timings.checkout = checkout_started.elapsed();
        // The hard reset above moved the superproject only; force-sync the
        // byte-copied submodule work trees to the gitlinks the base commit
        // records (local: the clone carries the source's `.git/modules`).
        // Best-effort — a broken submodule degrades with a warning instead
        // of failing the whole provisioning.
        let has_submodules = crate::submodule::has_submodules(checkout_path);
        if has_submodules
            || !pre_reset_submodules.is_empty()
            || checkout_path.join(".git").join("modules").is_dir()
        {
            let submodule_started = Instant::now();
            if has_submodules {
                if let Err(e) = crate::repo_cache::update_checkout_submodules(checkout_path) {
                    tracing::warn!(
                        checkout = %checkout_path.display(),
                        error = %e,
                        "provision_cow_checkout: submodule sync failed; checkout provisioned with submodules as copied"
                    );
                }
            }
            // Work trees copied from the source tip that the checked-out
            // ref no longer registers are orphans: untracked nested repos
            // plus dead `.git/modules` entries. Same best-effort posture.
            if let Err(e) = remove_orphaned_submodules(checkout_path, &pre_reset_submodules) {
                tracing::warn!(
                    checkout = %checkout_path.display(),
                    error = %e,
                    "provision_cow_checkout: orphaned submodule cleanup failed; stale submodule leftovers may remain"
                );
            }
            // Closing sync (parity with the direct path's
            // `hydrate_submodules_from_cache`): re-point recorded submodule
            // URLs at what `.gitmodules` says, so a divergent configured
            // URL copied from the source never sticks.
            if has_submodules {
                if let Err(e) = crate::repo_cache::sync_submodule_urls(checkout_path) {
                    tracing::warn!(
                        checkout = %checkout_path.display(),
                        error = %e,
                        "provision_cow_checkout: submodule URL sync failed; checkout may keep the source's submodule URLs"
                    );
                }
            }
            timings.submodule_update = submodule_started.elapsed();
        }
        // AFTER checkout: removing a self-referencing origin drops
        // `refs/remotes/origin/*`, which base-ref resolution above tries
        // first — same ordering as `provision_plain_clone_checkout`.
        resolve_inherited_origin(repo_path, checkout_path)?;
        Ok(sha)
    })()
    .inspect_err(|_| {
        let _ = std::fs::remove_dir_all(checkout_path);
    })?;
    timings.total = started.elapsed();
    Ok((result, timings))
}

/// A cloned main repository carries the source's `.git/worktrees/<name>/`
/// registrations, which point at the ORIGINAL repo's working trees: they make
/// the clone refuse to check out branches "already checked out" in the
/// source's linked worktrees, and pruning them from the clone could touch the
/// original's trees. Remove them from the clone before branching/checkout.
/// The source repository is never modified.
fn strip_worktree_registrations(checkout_path: &Path) -> Result<()> {
    let worktrees_dir = checkout_path.join(".git").join("worktrees");
    if worktrees_dir.is_dir() {
        std::fs::remove_dir_all(&worktrees_dir).map_err(|e| {
            Error::Internal(format!(
                "cannot remove stale worktree registrations from clone: {e}"
            ))
        })?;
        tracing::debug!(
            checkout = %checkout_path.display(),
            "provision_cow_checkout: removed stale .git/worktrees registrations from clone"
        );
    }
    Ok(())
}

/// Remove submodule work trees the byte copy carried from the source tip
/// that the checked-out state no longer registers, plus their now-dead
/// `.git/modules` entries. `pre_reset` is the recursive submodule path set
/// captured before the branch switch/hard reset (nested paths like
/// `sub/inner` included); orphans are that set minus the post-reset
/// recursive set — which also catches a nested submodule dropped by its
/// parent's own sync while the parent survives. Each orphan is a nested
/// repository, which both the hard reset and a plain superproject clean
/// skip — the identified paths are removed explicitly instead of via a
/// blanket `clean -ffdx`, which would also delete the legitimate untracked
/// files the CoW copy intentionally preserves. A path the checked-out
/// state still tracks as ordinary content (a submodule turned plain
/// directory/file) is left alone: removing it would delete tracked files
/// the reset just wrote. Iteration is in sorted order, so a removed parent
/// simply makes its nested orphans vanish from disk before their turn.
fn remove_orphaned_submodules(
    checkout_path: &Path,
    pre_reset: &std::collections::BTreeSet<String>,
) -> Result<()> {
    if pre_reset.is_empty() {
        // No submodule was registered pre-reset, but the byte copy can
        // still carry dead `.git/modules` dirs from the source (e.g. a
        // cache refreshed before pruning existed whose upstream removed
        // its last submodule) — same parity prune as the non-empty path.
        return crate::repo_cache::prune_stale_modules(checkout_path);
    }
    let post_reset = crate::submodule::recursive_submodule_paths(checkout_path)?;
    for orphan in pre_reset.difference(&post_reset) {
        let Some(rel) = crate::repo_cache::safe_rel_path(orphan) else {
            continue;
        };
        if is_tracked_content(checkout_path, &post_reset, orphan, &rel) {
            continue;
        }
        let target = checkout_path.join(&rel);
        let Ok(meta) = std::fs::symlink_metadata(&target) else {
            continue;
        };
        let removed = if meta.is_dir() {
            std::fs::remove_dir_all(&target)
        } else {
            std::fs::remove_file(&target)
        };
        removed.map_err(|e| {
            Error::Internal(format!(
                "cannot remove orphaned submodule work tree {orphan}: {e}"
            ))
        })?;
        tracing::debug!(
            checkout = %checkout_path.display(),
            submodule = %orphan,
            "provision_cow_checkout: removed orphaned submodule work tree"
        );
    }
    // The orphans' module git dirs under `.git/modules` are dead now that
    // the checked-out ref no longer names them; prune with the same
    // liveness rule as the cache refresh.
    crate::repo_cache::prune_stale_modules(checkout_path)
}

/// Whether the checked-out state tracks `orphan` as ordinary content (a
/// submodule turned plain directory/file). Checked against the HEAD tree
/// of the deepest post-reset submodule containing the orphan — for a
/// nested candidate like `sub/inner` the superproject's tree only holds
/// the `sub` gitlink, so the containing submodule's own tree is the one
/// that can track `inner` as content. Unreadable state answers `false`:
/// the candidate was a registered submodule moments ago, so removal is the
/// safe default.
fn is_tracked_content(
    checkout_path: &Path,
    post_reset: &std::collections::BTreeSet<String>,
    orphan: &str,
    rel: &Path,
) -> bool {
    let container = post_reset
        .iter()
        .filter(|sm| {
            orphan
                .strip_prefix(sm.as_str())
                .is_some_and(|r| r.starts_with('/'))
        })
        .max_by_key(|sm| sm.len());
    let (repo_path, inner_rel) = match container {
        Some(sm) => {
            let Some(sm_rel) = crate::repo_cache::safe_rel_path(sm) else {
                return false;
            };
            (
                checkout_path.join(sm_rel),
                Path::new(&orphan[sm.len() + 1..]).to_path_buf(),
            )
        }
        None => (checkout_path.to_path_buf(), rel.to_path_buf()),
    };
    let Ok(repo) = Repository::open(&repo_path) else {
        return false;
    };
    repo.head()
        .ok()
        .and_then(|h| h.peel_to_tree().ok())
        .is_some_and(|tree| tree.get_path(&inner_rel).is_ok())
}

/// The CoW clone is a byte-for-byte copy, so it inherits the source's
/// `origin` remote verbatim — both the fetch URL and any
/// `remote.origin.pushurl`. Resolve each via [`resolve_source_origin`] so the
/// clone's `origin` is valid from its own directory: a relative local path is
/// absolutized against the SOURCE repository, a local path resolving to the
/// source checkout itself is removed (the whole remote for the fetch URL, the
/// `pushurl` entry alone for the push URL), and network URLs /
/// already-absolute local paths are left verbatim. A source with no `origin`
/// is a no-op.
fn resolve_inherited_origin(source_repo: &Path, checkout_path: &Path) -> Result<()> {
    let clone = Repository::open(checkout_path).map_err(map_git_err)?;
    let Ok(remote) = clone.find_remote("origin") else {
        return Ok(());
    };
    let fetch_url = remote.url().map(str::to_owned).ok();
    let push_url = remote.pushurl().ok().flatten().map(str::to_owned);
    drop(remote);
    if let Some(url) = fetch_url {
        match resolve_source_origin(source_repo, &url) {
            Some(resolved) => {
                if resolved != url {
                    clone
                        .remote_set_url("origin", &resolved)
                        .map_err(map_git_err)?;
                }
            }
            None => {
                // Deleting the remote drops its pushurl with it.
                clone.remote_delete("origin").map_err(map_git_err)?;
                return Ok(());
            }
        }
    }
    if let Some(url) = push_url {
        match resolve_source_origin(source_repo, &url) {
            Some(resolved) => {
                if resolved != url {
                    clone
                        .remote_set_pushurl("origin", Some(&resolved))
                        .map_err(map_git_err)?;
                }
            }
            None => {
                clone
                    .remote_set_pushurl("origin", None)
                    .map_err(map_git_err)?;
            }
        }
    }
    Ok(())
}

/// Branch + checkout + hard reset inside the freshly cloned repository.
/// Shared with [`crate::repo_cache::provision_direct_checkout`], which needs
/// the identical base-ref resolution + branch-reuse semantics in a plain
/// local clone.
pub(crate) fn checkout_in_clone(
    checkout_path: &Path,
    branch: &str,
    base_ref: Option<&str>,
    remote: &str,
) -> Result<String> {
    let repo = Repository::open(checkout_path).map_err(map_git_err)?;

    // Resolve the base commit in the clone (a full copy of the source repo's
    // refs): remote-tracking ref, then local branch, then any rev-parsable
    // spec (tag/SHA); no base_ref means HEAD.
    let base_commit = match base_ref.filter(|r| !r.is_empty()) {
        Some(r) => [
            format!("refs/remotes/{remote}/{r}"),
            format!("refs/heads/{r}"),
            r.to_string(),
        ]
        .iter()
        .find_map(|spec| repo.revparse_single(spec).ok())
        .and_then(|obj| obj.peel_to_commit().ok())
        .ok_or_else(|| Error::BaseRefUnresolvable {
            base_ref: r.to_string(),
        })?,
        None => repo
            .head()
            .and_then(|h| h.peel_to_commit())
            .map_err(map_git_err)?,
    };

    // Create the branch at the base commit, or reuse an existing branch of
    // the same name (provision_worktree parity).
    let branch_ref = match repo.find_branch(branch, BranchType::Local) {
        Ok(b) => b.into_reference(),
        Err(_) => repo
            .branch(branch, &base_commit, false)
            .map_err(map_git_err)?
            .into_reference(),
    };
    let target = branch_ref.peel_to_commit().map_err(map_git_err)?;
    let checked_out_sha = target.id().to_string();

    // Point HEAD at the branch, then hard-reset tracked files to its commit.
    // A hard reset discards tracked modifications carried over from the
    // source working tree but leaves untracked files in place.
    let refname = format!("refs/heads/{branch}");
    repo.set_head(&refname).map_err(map_git_err)?;
    repo.reset(target.as_object(), git2::ResetType::Hard, None)
        .map_err(map_git_err)?;
    Ok(checked_out_sha)
}

/// Provision a **standalone** plain-clone checkout from an arbitrary local
/// source repository — the non-CoW fallback for `workspace.duplicate` of a
/// standalone (`cow`/`direct`) source (intent-hq/monorepo#1560).
///
/// A duplicate of a standalone workspace must never hold a live filesystem
/// reference into the source workspace's directory (a linked worktree rooted
/// there is orphaned when the source is deleted, and deleting the duplicate
/// mutates the source). This produces a self-contained repository instead:
///
/// 1. `git clone <source_repo> <checkout_path>` — a plain local clone
///    (hardlinked objects where the filesystem allows). Committed state only;
///    uncommitted/untracked work in the source is not carried over.
/// 2. Overlay the source's own remote-tracking refs
///    (`+refs/remotes/origin/*:refs/remotes/origin/*`) so `base_ref`
///    resolution sees every upstream branch, not just the source's local ones.
/// 3. Create + check out `branch` from `base_ref` via [`checkout_in_clone`]
///    (same base-ref resolution and branch-reuse semantics as the CoW path)
///    and hard-reset to it.
/// 4. Retarget `origin` at the source's own `origin` URL, resolved so it is
///    valid from the duplicate's directory ([`resolve_source_origin`]). When
///    the source has no usable `origin` (e.g. an `isNewRepo` local-only repo,
///    or an `origin` that is the source checkout itself), the clone's `origin`
///    remote is **removed** so no config value references the source path.
///
/// Returns the SHA the checkout lands on. On failure after the clone, the
/// partially provisioned `checkout_path` is removed best-effort. Blocking —
/// callers run it on the blocking pool.
pub fn provision_local_clone_checkout(
    source_repo: &Path,
    checkout_path: &Path,
    branch: &str,
    base_ref: Option<&str>,
) -> Result<String> {
    // Read the source's upstream URL before cloning: the clone's own `origin`
    // points at `source_repo`, so it cannot answer this question itself.
    let source = Repository::open(source_repo).map_err(map_git_err)?;
    let source_origin_url = source
        .find_remote("origin")
        .ok()
        .and_then(|r| r.url().map(str::to_owned).ok());
    drop(source);
    let resolved_origin = source_origin_url
        .as_deref()
        .and_then(|url| resolve_source_origin(source_repo, url));
    let origin = match resolved_origin.as_deref() {
        Some(url) => OriginTarget::Url(url),
        None => OriginTarget::Remove,
    };
    provision_plain_clone_checkout(source_repo, checkout_path, origin, branch, base_ref)
}

/// The local filesystem path a remote URL denotes, or `None` when the URL is
/// network-addressed (and therefore location-independent).
///
/// Git accepts local repositories both as bare paths (`../upstream`,
/// `/srv/git/r`) and as `file://` URLs; everything else — an explicit scheme
/// (`https://`, `ssh://`, `git://`) or the scp-like `user@host:path` shorthand
/// — is remote. A single-letter segment before the colon is a Windows drive
/// (`C:\repos\r`), not an scp host.
fn local_origin_path(url: &str) -> Option<&str> {
    if let Some(rest) = url.strip_prefix("file://") {
        return Some(rest);
    }
    if url.contains("://") {
        return None;
    }
    if let Some(colon) = url.find(':') {
        let host = &url[..colon];
        let is_windows_drive = host.len() == 1 && host.chars().all(|c| c.is_ascii_alphabetic());
        if !is_windows_drive && !host.contains('/') && !host.contains('\\') {
            return None;
        }
    }
    Some(url)
}

/// Resolve the source's `origin` URL into a value that still means the same
/// thing from the duplicate's own directory, or `None` when `origin` must be
/// dropped instead.
///
/// A network URL, and a local path that is already absolute, are carried over
/// verbatim. The two cases that cannot be:
///
/// - A **relative** local path (`../upstream`) resolves against the repository
///   holding it, so verbatim it would re-resolve against the duplicate's
///   directory and name something else entirely. It is absolutized.
/// - A local path resolving to `source_repo` itself means the source checkout
///   *is* the upstream; keeping it would leave the duplicate depending on a
///   directory that disappears with the source workspace (monorepo#1560), so
///   the remote is dropped.
fn resolve_source_origin(source_repo: &Path, url: &str) -> Option<String> {
    let Some(local) = local_origin_path(url) else {
        return Some(url.to_string());
    };
    let local = Path::new(local);
    let canonical = |p: &Path| std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf());
    let resolved = canonical(&source_repo.join(local));
    if resolved == canonical(source_repo) {
        return None;
    }
    if local.is_absolute() {
        return Some(url.to_string());
    }
    Some(resolved.to_string_lossy().into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cow::{cow_probe, CowSupport};
    use crate::testutil::{commit_file, init_repo, write_file, TempDir};

    /// Skip guard: CoW cloning depends on the filesystem under `TMPDIR` (CI
    /// may run on non-CoW filesystems). Returns `true` when supported.
    fn cow_available(src: &std::path::Path) -> bool {
        let dst = std::env::temp_dir();
        match cow_probe(src, &dst) {
            Ok(CowSupport::Supported) => true,
            _ => {
                eprintln!("Skipping test: CoW not supported under {dst:?}");
                false
            }
        }
    }

    /// `testutil::init_repo` at a caller-chosen path, for tests that need two
    /// repositories in a known relative layout (`../upstream`).
    fn init_repo_at(path: &std::path::Path) {
        std::fs::create_dir_all(path).unwrap();
        let repo = Repository::init(path).unwrap();
        let mut cfg = repo.config().unwrap();
        cfg.set_str("user.name", "Test").unwrap();
        cfg.set_str("user.email", "test@example.com").unwrap();
    }

    fn unique_checkout(tag: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("cow-checkout-{tag}-{nanos}"))
    }

    /// Drop guard for a provisioned checkout directory.
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn head_sha(dir: &TempDir) -> String {
        let repo = Repository::open(dir.path()).unwrap();
        let sha = repo.head().unwrap().target().unwrap().to_string();
        sha
    }

    fn head_branch(dir: &TempDir) -> String {
        let repo = Repository::open(dir.path()).unwrap();
        let name = repo.head().unwrap().shorthand().unwrap().to_string();
        name
    }

    #[test]
    fn provisions_standalone_checkout_on_new_branch_from_base_ref() {
        let dir = init_repo("cowchk-base");
        commit_file(dir.path(), "a.txt", "x\n");
        if !cow_available(dir.path()) {
            return;
        }
        // Pin `base` at the first commit, then advance HEAD past it.
        let base_sha = head_sha(&dir);
        crate::testutil::create_branch(dir.path(), "base");
        commit_file(dir.path(), "b.txt", "y\n");

        let checkout = unique_checkout("base");
        let _cleanup = Cleanup(checkout.clone());
        let sha =
            provision_cow_checkout(dir.path(), &checkout, "cow-ws", Some("base"), "origin", &[])
                .unwrap();
        assert_eq!(sha, base_sha);

        let clone = Repository::open(&checkout).unwrap();
        assert!(!clone.is_worktree(), "CoW checkout is a standalone repo");
        let head = clone.head().unwrap();
        assert_eq!(head.shorthand().expect("branch name"), "cow-ws");
        assert_eq!(head.target().unwrap().to_string(), base_sha);
        // Tracked files match the base commit, not the source HEAD.
        assert_eq!(
            std::fs::read_to_string(checkout.join("a.txt")).unwrap(),
            "x\n"
        );
        assert!(!checkout.join("b.txt").exists());
    }

    #[test]
    fn preserves_untracked_files_and_resets_dirty_tracked_files() {
        let dir = init_repo("cowchk-dirty");
        commit_file(dir.path(), "a.txt", "x\n");
        if !cow_available(dir.path()) {
            return;
        }
        let branch = head_branch(&dir);
        // Dirty the tracked file and add an untracked build artifact.
        write_file(dir.path(), "a.txt", "dirty\n");
        write_file(dir.path(), "target/build.log", "artifact\n");

        let checkout = unique_checkout("dirty");
        let _cleanup = Cleanup(checkout.clone());
        provision_cow_checkout(
            dir.path(),
            &checkout,
            "cow-ws",
            Some(&branch),
            "origin",
            &[],
        )
        .unwrap();

        // Tracked file is reset to the base commit; untracked artifact survives.
        assert_eq!(
            std::fs::read_to_string(checkout.join("a.txt")).unwrap(),
            "x\n"
        );
        assert_eq!(
            std::fs::read_to_string(checkout.join("target/build.log")).unwrap(),
            "artifact\n"
        );
        // The source repo is untouched (still dirty).
        assert_eq!(
            std::fs::read_to_string(dir.path().join("a.txt")).unwrap(),
            "dirty\n"
        );
    }

    #[test]
    fn reuses_existing_branch_of_same_name() {
        let dir = init_repo("cowchk-reuse");
        commit_file(dir.path(), "a.txt", "x\n");
        if !cow_available(dir.path()) {
            return;
        }
        // `cow-ws` pinned at the first commit; HEAD advances past it.
        let pinned_sha = head_sha(&dir);
        crate::testutil::create_branch(dir.path(), "cow-ws");
        commit_file(dir.path(), "b.txt", "y\n");
        let base = head_branch(&dir);

        let checkout = unique_checkout("reuse");
        let _cleanup = Cleanup(checkout.clone());
        let sha =
            provision_cow_checkout(dir.path(), &checkout, "cow-ws", Some(&base), "origin", &[])
                .unwrap();
        assert_eq!(sha, pinned_sha, "existing branch is reused, not recreated");
    }

    #[test]
    fn refuses_linked_worktree_source_as_unsupported() {
        // Case A: the source repo is itself a linked git worktree (its `.git`
        // is a gitfile). CoW-cloning it would corrupt the original checkout,
        // so provisioning must refuse with Unsupported BEFORE cloning.
        let dir = init_repo("cowchk-gitfile");
        commit_file(dir.path(), "a.txt", "x\n");
        let branch = head_branch(&dir);

        // Create a linked worktree of the repo and use IT as the source.
        let wt_path = unique_checkout("gitfile-wt");
        let _wt_cleanup = Cleanup(wt_path.clone());
        crate::worktree::provision_worktree(
            dir.path(),
            "gitfile-wt",
            &wt_path,
            "wt-branch",
            Some(&branch),
            "origin",
        )
        .unwrap();
        assert!(wt_path.join(".git").is_file(), "worktree .git is a gitfile");

        let checkout = unique_checkout("gitfile-dst");
        let _cleanup = Cleanup(checkout.clone());
        let err =
            provision_cow_checkout(&wt_path, &checkout, "cow-ws", Some(&branch), "origin", &[])
                .unwrap_err();
        assert!(matches!(err, Error::Unsupported(_)), "got: {err:?}");
        assert!(!checkout.exists(), "nothing is cloned for a gitfile source");
        // The source worktree is untouched.
        assert_eq!(
            std::fs::read_to_string(wt_path.join("a.txt")).unwrap(),
            "x\n"
        );
    }

    #[test]
    fn strips_stale_worktree_registrations_from_clone() {
        // Case B: the source main repo has linked worktrees; the clone
        // inherits `.git/worktrees/<name>` registrations pointing at the
        // ORIGINAL repo's trees. They must be stripped from the clone so it
        // can check out branches "already checked out" in the source's
        // worktrees — without modifying the source.
        let dir = init_repo("cowchk-strip");
        commit_file(dir.path(), "a.txt", "x\n");
        if !cow_available(dir.path()) {
            return;
        }
        let base = head_branch(&dir);

        // Register a linked worktree on branch `busy` in the source repo.
        let wt_path = unique_checkout("strip-wt");
        let _wt_cleanup = Cleanup(wt_path.clone());
        crate::worktree::provision_worktree(
            dir.path(),
            "strip-wt",
            &wt_path,
            "busy",
            Some(&base),
            "origin",
        )
        .unwrap();
        assert!(dir.path().join(".git/worktrees").is_dir());

        // Provision a CoW checkout ON the branch held by the source's linked
        // worktree; without the strip, git refuses ("already checked out").
        let checkout = unique_checkout("strip-dst");
        let _cleanup = Cleanup(checkout.clone());
        provision_cow_checkout(dir.path(), &checkout, "busy", Some(&base), "origin", &[]).unwrap();

        let clone = Repository::open(&checkout).unwrap();
        assert_eq!(clone.head().unwrap().shorthand().unwrap(), "busy");
        assert!(
            !checkout.join(".git/worktrees").exists(),
            "stale registrations are stripped from the clone"
        );
        // The source repo's registrations and worktree are untouched.
        assert!(dir.path().join(".git/worktrees").is_dir());
        assert!(wt_path.join(".git").is_file());
    }

    #[test]
    fn rejects_unresolvable_base_ref_and_cleans_up() {
        let dir = init_repo("cowchk-badref");
        commit_file(dir.path(), "a.txt", "x\n");
        if !cow_available(dir.path()) {
            return;
        }
        let checkout = unique_checkout("badref");
        let err = provision_cow_checkout(
            dir.path(),
            &checkout,
            "cow-ws",
            Some("no-such-ref"),
            "origin",
            &[],
        )
        .unwrap_err();
        assert!(
            matches!(err, Error::BaseRefUnresolvable { ref base_ref } if base_ref == "no-such-ref")
        );
        assert!(!checkout.exists(), "partial checkout is removed on failure");
    }

    #[test]
    fn clone_excludes_skip_untracked_directories() {
        let dir = init_repo("cowchk-exclude");
        commit_file(dir.path(), "a.txt", "x\n");
        if !cow_available(dir.path()) {
            return;
        }
        let branch = head_branch(&dir);
        // Untracked heavy directories: one excluded, one kept.
        write_file(dir.path(), "node_modules/pkg/index.js", "js\n");
        write_file(dir.path(), "vendor/keep.txt", "keep\n");

        let checkout = unique_checkout("exclude");
        let _cleanup = Cleanup(checkout.clone());
        let (_, timings) = provision_cow_checkout_timed(
            dir.path(),
            &checkout,
            "cow-ws",
            Some(&branch),
            "origin",
            &["node_modules".to_string()],
        )
        .unwrap();

        assert!(
            !checkout.join("node_modules").exists(),
            "excluded directory is absent from the checkout"
        );
        assert_eq!(
            std::fs::read_to_string(checkout.join("vendor/keep.txt")).unwrap(),
            "keep\n"
        );
        assert_eq!(
            std::fs::read_to_string(checkout.join("a.txt")).unwrap(),
            "x\n"
        );
        assert_eq!(timings.skipped_excluded, 1);
        assert!(
            !timings.whole_tree_clone,
            "exclusions force the best-effort walk"
        );
        // The source repo keeps its directory.
        assert!(dir.path().join("node_modules/pkg/index.js").exists());
    }

    #[test]
    fn git_exclusion_is_ignored_and_clone_stays_valid() {
        let dir = init_repo("cowchk-gitexclude");
        commit_file(dir.path(), "a.txt", "x\n");
        if !cow_available(dir.path()) {
            return;
        }
        let branch = head_branch(&dir);

        let checkout = unique_checkout("gitexclude");
        let _cleanup = Cleanup(checkout.clone());
        let (_, timings) = provision_cow_checkout_timed(
            dir.path(),
            &checkout,
            "cow-ws",
            Some(&branch),
            "origin",
            &[".git".to_string(), ".git/objects".to_string()],
        )
        .unwrap();

        // `.git` exclusions are ignored: the clone is a working repository.
        assert_eq!(timings.skipped_excluded, 0);
        let clone = Repository::open(&checkout).unwrap();
        assert_eq!(clone.head().unwrap().shorthand().unwrap(), "cow-ws");
    }

    #[test]
    fn timed_provisioning_reports_phase_durations() {
        let dir = init_repo("cowchk-timing");
        commit_file(dir.path(), "a.txt", "x\n");
        if !cow_available(dir.path()) {
            return;
        }
        let branch = head_branch(&dir);

        let checkout = unique_checkout("timing");
        let _cleanup = Cleanup(checkout.clone());
        let (sha, timings) = provision_cow_checkout_timed(
            dir.path(),
            &checkout,
            "cow-ws",
            Some(&branch),
            "origin",
            &[],
        )
        .unwrap();

        assert!(!sha.is_empty());
        assert!(timings.total >= timings.cow_clone);
        assert!(timings.total >= timings.checkout);
        assert!(timings.total > std::time::Duration::ZERO);
        assert_eq!(
            timings.submodule_update,
            Duration::ZERO,
            "a repo without submodules skips the submodule phase entirely"
        );
        // The display helper renders one entry per recorded subtree.
        assert_eq!(
            timings.slowest_subtrees_display().is_empty(),
            timings.slowest_subtrees.is_empty()
        );
    }

    /// A **relative** local-path `origin` (`../upstream`) inherited by the
    /// CoW clone resolves against the clone's own directory, where it names
    /// something else entirely. It is absolutized against the SOURCE so it
    /// still names the same upstream (monorepo#1582).
    #[test]
    fn cow_checkout_absolutizes_a_relative_local_origin() {
        // `<tmp>/<parent>/{upstream,source}`, so `../upstream` is meaningful
        // from the source but not from the clone's own directory.
        let parent = unique_checkout("cowchk-relparent");
        let _cleanup_parent = Cleanup(parent.clone());
        std::fs::create_dir_all(&parent).unwrap();
        let upstream = parent.join("upstream");
        init_repo_at(&upstream);
        commit_file(&upstream, "a.txt", "one\n");
        let source = parent.join("source");
        init_repo_at(&source);
        {
            let repo = Repository::open(&source).unwrap();
            repo.remote("origin", "../upstream").unwrap();
        }
        commit_file(&source, "a.txt", "one\n");
        if !cow_available(&source) {
            return;
        }

        let checkout = unique_checkout("cowchk-relorigin");
        let _cleanup = Cleanup(checkout.clone());
        provision_cow_checkout(&source, &checkout, "cow-ws", None, "origin", &[]).unwrap();

        let clone = Repository::open(&checkout).unwrap();
        let origin = clone.find_remote("origin").unwrap();
        let url = std::path::Path::new(origin.url().unwrap());
        assert!(url.is_absolute(), "relative origin must be absolutized");
        assert_eq!(
            std::fs::canonicalize(url).unwrap(),
            std::fs::canonicalize(&upstream).unwrap(),
            "absolutized origin still names the same upstream"
        );
        assert_self_contained(&checkout, &source);
    }

    /// A local-path `origin` resolving to the source checkout ITSELF (so the
    /// source is its own upstream): the inherited remote is removed rather
    /// than pinning the clone to a directory that dies with the source
    /// (monorepo#1582).
    #[test]
    fn cow_checkout_removes_origin_pointing_at_the_source_itself() {
        let source = init_repo("cowchk-selforigin");
        commit_file(source.path(), "a.txt", "one\n");
        {
            let repo = Repository::open(source.path()).unwrap();
            repo.remote("origin", &source.path().display().to_string())
                .unwrap();
        }
        if !cow_available(source.path()) {
            return;
        }

        let checkout = unique_checkout("cowchk-selforigin");
        let _cleanup = Cleanup(checkout.clone());
        provision_cow_checkout(source.path(), &checkout, "cow-ws", None, "origin", &[]).unwrap();

        let clone = Repository::open(&checkout).unwrap();
        assert!(
            clone.find_remote("origin").is_err(),
            "an origin naming the source checkout must be removed"
        );
        assert_self_contained(&checkout, source.path());
    }

    /// Origin removal must not break base-ref resolution: a self-referencing
    /// `origin` is removed (dropping `refs/remotes/origin/*` with it) only
    /// AFTER checkout, so a `base_ref` surviving solely as a remote-tracking
    /// ref still resolves — same ordering as the plain-clone path.
    #[test]
    fn cow_checkout_resolves_remote_tracking_base_ref_before_removing_origin() {
        let source = init_repo("cowchk-selforigin-remoteref");
        commit_file(source.path(), "a.txt", "one\n");
        let pinned_sha = head_sha(&source);
        {
            let repo = Repository::open(source.path()).unwrap();
            repo.remote("origin", &source.path().display().to_string())
                .unwrap();
            // A branch that exists ONLY as a remote-tracking ref (as if
            // fetched once and then deleted locally).
            let oid = git2::Oid::from_str(&pinned_sha).unwrap();
            repo.reference("refs/remotes/origin/only-remote", oid, false, "test")
                .unwrap();
        }
        commit_file(source.path(), "b.txt", "two\n");
        if !cow_available(source.path()) {
            return;
        }

        let checkout = unique_checkout("cowchk-selforigin-remoteref");
        let _cleanup = Cleanup(checkout.clone());
        let sha = provision_cow_checkout(
            source.path(),
            &checkout,
            "cow-ws",
            Some("only-remote"),
            "origin",
            &[],
        )
        .unwrap();

        assert_eq!(
            sha, pinned_sha,
            "base_ref resolves via the remote-tracking ref before origin removal"
        );
        let clone = Repository::open(&checkout).unwrap();
        assert_eq!(clone.head().unwrap().shorthand().unwrap(), "cow-ws");
        assert!(
            clone.find_remote("origin").is_err(),
            "the self-referencing origin is still removed afterwards"
        );
        assert_self_contained(&checkout, source.path());
    }

    /// A network-URL `origin` is location-independent, so the CoW clone
    /// carries it over verbatim.
    #[test]
    fn cow_checkout_carries_network_origin_verbatim() {
        let source = init_repo("cowchk-neturl");
        commit_file(source.path(), "a.txt", "one\n");
        let network_url = "https://example.com/upstream.git";
        {
            let repo = Repository::open(source.path()).unwrap();
            repo.remote("origin", network_url).unwrap();
        }
        if !cow_available(source.path()) {
            return;
        }

        let checkout = unique_checkout("cowchk-neturl");
        let _cleanup = Cleanup(checkout.clone());
        provision_cow_checkout(source.path(), &checkout, "cow-ws", None, "origin", &[]).unwrap();

        let clone = Repository::open(&checkout).unwrap();
        assert_eq!(
            clone.find_remote("origin").unwrap().url().unwrap(),
            network_url,
            "network origin carries over verbatim"
        );
        assert_self_contained(&checkout, source.path());
    }

    /// A **relative** `remote.origin.pushurl` (`../upstream`) inherited by
    /// the CoW clone re-resolves against the clone's own directory, so it is
    /// absolutized against the SOURCE just like the fetch URL; the network
    /// fetch URL itself carries over verbatim.
    #[test]
    fn cow_checkout_absolutizes_a_relative_pushurl() {
        // `<tmp>/<parent>/{upstream,source}`, so `../upstream` is meaningful
        // from the source but not from the clone's own directory.
        let parent = unique_checkout("cowchk-pushrelparent");
        let _cleanup_parent = Cleanup(parent.clone());
        std::fs::create_dir_all(&parent).unwrap();
        let upstream = parent.join("upstream");
        init_repo_at(&upstream);
        commit_file(&upstream, "a.txt", "one\n");
        let source = parent.join("source");
        init_repo_at(&source);
        let network_url = "https://example.com/upstream.git";
        {
            let repo = Repository::open(&source).unwrap();
            repo.remote("origin", network_url).unwrap();
            repo.remote_set_pushurl("origin", Some("../upstream"))
                .unwrap();
        }
        commit_file(&source, "a.txt", "one\n");
        if !cow_available(&source) {
            return;
        }

        let checkout = unique_checkout("cowchk-pushrel");
        let _cleanup = Cleanup(checkout.clone());
        provision_cow_checkout(&source, &checkout, "cow-ws", None, "origin", &[]).unwrap();

        let clone = Repository::open(&checkout).unwrap();
        let origin = clone.find_remote("origin").unwrap();
        assert_eq!(
            origin.url().unwrap(),
            network_url,
            "network fetch URL carries over verbatim"
        );
        let pushurl = std::path::Path::new(origin.pushurl().unwrap().unwrap());
        assert!(
            pushurl.is_absolute(),
            "relative pushurl must be absolutized"
        );
        assert_eq!(
            std::fs::canonicalize(pushurl).unwrap(),
            std::fs::canonicalize(&upstream).unwrap(),
            "absolutized pushurl still names the same upstream"
        );
        assert_self_contained(&checkout, &source);
    }

    /// A `remote.origin.pushurl` resolving to the source checkout ITSELF:
    /// the inherited pushurl entry is unset (the fetch URL, here a network
    /// URL, stays) rather than leaving pushes targeting a directory that
    /// dies with the source.
    #[test]
    fn cow_checkout_removes_a_pushurl_pointing_at_the_source_itself() {
        let source = init_repo("cowchk-selfpush");
        commit_file(source.path(), "a.txt", "one\n");
        let network_url = "https://example.com/upstream.git";
        {
            let repo = Repository::open(source.path()).unwrap();
            repo.remote("origin", network_url).unwrap();
            repo.remote_set_pushurl("origin", Some(&source.path().display().to_string()))
                .unwrap();
        }
        if !cow_available(source.path()) {
            return;
        }

        let checkout = unique_checkout("cowchk-selfpush");
        let _cleanup = Cleanup(checkout.clone());
        provision_cow_checkout(source.path(), &checkout, "cow-ws", None, "origin", &[]).unwrap();

        let clone = Repository::open(&checkout).unwrap();
        let origin = clone.find_remote("origin").unwrap();
        assert_eq!(
            origin.url().unwrap(),
            network_url,
            "network fetch URL carries over verbatim"
        );
        assert!(
            origin.pushurl().unwrap().is_none(),
            "a pushurl naming the source checkout must be removed"
        );
        assert_self_contained(&checkout, source.path());
    }

    /// Every value in the clone's own (local) config, so a test can assert
    /// nothing still points at the source checkout's path.
    fn local_config_values(checkout: &std::path::Path) -> Vec<String> {
        let cfg = git2::Config::open(&checkout.join(".git").join("config")).unwrap();
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

    /// The clone must be a self-contained repository: a real `.git` directory
    /// (not a worktree gitfile) and no config value naming the source path.
    fn assert_self_contained(checkout: &std::path::Path, source: &std::path::Path) {
        assert!(
            checkout.join(".git").is_dir(),
            "clone must own a real .git directory, not a gitfile"
        );
        let source_str = source.display().to_string();
        for value in local_config_values(checkout) {
            assert!(
                !value.contains(&source_str),
                "config value {value:?} still references the source path"
            );
        }
    }

    /// Duplicating a standalone source with an `origin`: the clone is a
    /// self-contained repo on the workspace branch with `origin` retargeted at
    /// the source's own upstream URL, never the source path.
    #[test]
    fn local_clone_retargets_origin_to_the_source_upstream() {
        let upstream = init_repo("localclone-upstream");
        commit_file(upstream.path(), "a.txt", "one\n");
        let source = init_repo("localclone-src");
        let upstream_url = format!("file://{}", upstream.path().display());
        {
            let repo = Repository::open(source.path()).unwrap();
            repo.remote("origin", &upstream_url).unwrap();
        }
        commit_file(source.path(), "a.txt", "one\n");
        let source_sha = head_sha(&source);

        let checkout = unique_checkout("localclone-origin");
        let _cleanup = Cleanup(checkout.clone());
        let sha = provision_local_clone_checkout(source.path(), &checkout, "dup-ws", None).unwrap();

        assert_eq!(sha, source_sha);
        let clone = Repository::open(&checkout).unwrap();
        assert!(!clone.is_worktree(), "duplicate is a standalone repo");
        assert_eq!(clone.head().unwrap().shorthand().unwrap(), "dup-ws");
        assert_eq!(
            clone.find_remote("origin").unwrap().url().unwrap(),
            upstream_url.as_str()
        );
        assert_eq!(
            std::fs::read_to_string(checkout.join("a.txt")).unwrap(),
            "one\n"
        );
        assert_self_contained(&checkout, source.path());
    }

    /// A **relative** local-path `origin` (`../upstream`) resolves against the
    /// repository holding it, so carrying it over verbatim would re-resolve it
    /// against the duplicate's directory. It is absolutized so it still names
    /// the same upstream, and stays fetchable from the duplicate.
    #[test]
    fn local_clone_absolutizes_a_relative_local_origin() {
        // `<tmp>/<parent>/{upstream,source}`, so `../upstream` is meaningful
        // from the source but not from the duplicate's own directory.
        let parent = unique_checkout("localclone-relparent");
        let _cleanup_parent = Cleanup(parent.clone());
        std::fs::create_dir_all(&parent).unwrap();
        let upstream = parent.join("upstream");
        init_repo_at(&upstream);
        commit_file(&upstream, "a.txt", "one\n");
        let source = parent.join("source");
        init_repo_at(&source);
        {
            let repo = Repository::open(&source).unwrap();
            repo.remote("origin", "../upstream").unwrap();
        }
        commit_file(&source, "a.txt", "one\n");

        let checkout = unique_checkout("localclone-relorigin");
        let _cleanup = Cleanup(checkout.clone());
        provision_local_clone_checkout(&source, &checkout, "dup-ws", None).unwrap();

        let clone = Repository::open(&checkout).unwrap();
        let origin = clone.find_remote("origin").unwrap();
        let url = std::path::Path::new(origin.url().unwrap());
        assert!(url.is_absolute(), "relative origin must be absolutized");
        assert_eq!(
            std::fs::canonicalize(url).unwrap(),
            std::fs::canonicalize(&upstream).unwrap(),
            "absolutized origin still names the same upstream"
        );
        assert_self_contained(&checkout, &source);
    }

    /// A local-path `origin` that resolves to the source checkout ITSELF (so
    /// the source is its own upstream): the remote is dropped rather than
    /// pinning the duplicate to a directory that dies with the source.
    #[test]
    fn local_clone_removes_origin_pointing_at_the_source_itself() {
        let source = init_repo("localclone-selforigin");
        commit_file(source.path(), "a.txt", "one\n");
        {
            let repo = Repository::open(source.path()).unwrap();
            repo.remote("origin", &source.path().display().to_string())
                .unwrap();
        }

        let checkout = unique_checkout("localclone-selforigin");
        let _cleanup = Cleanup(checkout.clone());
        provision_local_clone_checkout(source.path(), &checkout, "dup-ws", None).unwrap();

        let clone = Repository::open(&checkout).unwrap();
        assert!(
            clone.find_remote("origin").is_err(),
            "an origin naming the source checkout must be removed"
        );
        assert_self_contained(&checkout, source.path());
    }

    /// Duplicating a source with no `origin` (an `isNewRepo` local-only repo):
    /// the clone's `origin` is removed rather than left pointing at the source.
    #[test]
    fn local_clone_removes_origin_when_source_has_none() {
        let source = init_repo("localclone-noorigin");
        commit_file(source.path(), "a.txt", "one\n");

        let checkout = unique_checkout("localclone-noorigin");
        let _cleanup = Cleanup(checkout.clone());
        let sha = provision_local_clone_checkout(source.path(), &checkout, "dup-ws", None).unwrap();

        assert_eq!(sha, head_sha(&source));
        let clone = Repository::open(&checkout).unwrap();
        assert!(
            clone.find_remote("origin").is_err(),
            "origin must be removed when the source has no upstream"
        );
        assert_self_contained(&checkout, source.path());
    }

    /// A `base_ref` naming a source branch starts the workspace branch there,
    /// not at the source's HEAD.
    #[test]
    fn local_clone_branches_from_base_ref() {
        let source = init_repo("localclone-base");
        commit_file(source.path(), "a.txt", "one\n");
        let base_sha = head_sha(&source);
        crate::testutil::create_branch(source.path(), "base");
        commit_file(source.path(), "a.txt", "two\n");

        let checkout = unique_checkout("localclone-base");
        let _cleanup = Cleanup(checkout.clone());
        let sha = provision_local_clone_checkout(source.path(), &checkout, "dup-ws", Some("base"))
            .unwrap();

        assert_eq!(sha, base_sha);
        assert_eq!(
            std::fs::read_to_string(checkout.join("a.txt")).unwrap(),
            "one\n",
            "tracked files match the base ref, not the source HEAD"
        );
        assert_self_contained(&checkout, source.path());
    }

    /// A branch name the clone already carries (the source's default branch)
    /// is reused rather than recreated — `provision_worktree` parity.
    #[test]
    fn local_clone_reuses_an_existing_branch_name() {
        let source = init_repo("localclone-reuse");
        commit_file(source.path(), "a.txt", "one\n");
        let branch = head_branch(&source);
        commit_file(source.path(), "a.txt", "two\n");
        let tip_sha = head_sha(&source);

        let checkout = unique_checkout("localclone-reuse");
        let _cleanup = Cleanup(checkout.clone());
        let sha = provision_local_clone_checkout(source.path(), &checkout, &branch, Some(&branch))
            .unwrap();

        assert_eq!(sha, tip_sha, "reused branch keeps its own tip");
        let clone = Repository::open(&checkout).unwrap();
        assert_eq!(clone.head().unwrap().shorthand().unwrap(), branch);
        assert_self_contained(&checkout, source.path());
    }

    /// An unresolvable `base_ref` surfaces the typed error and removes the
    /// partially provisioned checkout.
    #[test]
    fn local_clone_rejects_unresolvable_base_ref_and_cleans_up() {
        let source = init_repo("localclone-badref");
        commit_file(source.path(), "a.txt", "one\n");

        let checkout = unique_checkout("localclone-badref");
        let _cleanup = Cleanup(checkout.clone());
        let err = provision_local_clone_checkout(source.path(), &checkout, "dup-ws", Some("nope"))
            .unwrap_err();

        assert!(
            matches!(err, Error::BaseRefUnresolvable { ref base_ref } if base_ref == "nope"),
            "got: {err:?}"
        );
        assert!(!checkout.exists(), "partial checkout is removed on failure");
    }

    /// A source path that is not a repository fails before any clone work.
    #[test]
    fn local_clone_rejects_non_repository_source() {
        let source = std::env::temp_dir().join("localclone-not-a-repo");
        std::fs::create_dir_all(&source).unwrap();
        let _cleanup_src = Cleanup(source.clone());
        let checkout = unique_checkout("localclone-nonrepo");
        let _cleanup = Cleanup(checkout.clone());

        let err = provision_local_clone_checkout(&source, &checkout, "dup-ws", None).unwrap_err();
        assert!(matches!(err, Error::Internal(_)), "got: {err:?}");
        assert!(!checkout.exists());
    }

    /// `git clone --recurse-submodules` of `origin` into a fresh temp dir —
    /// the same shape as a repo cache: submodule work trees populated, module
    /// git dirs under `.git/modules`.
    fn recursive_clone(origin: &std::path::Path, tag: &str) -> std::path::PathBuf {
        let dst = unique_checkout(tag);
        let status = std::process::Command::new("git")
            .arg("clone")
            .arg("--recurse-submodules")
            .arg("--")
            .arg(origin)
            .arg(&dst)
            .status()
            .unwrap();
        assert!(status.success(), "recursive clone of the fixture failed");
        dst
    }

    fn sub_head(checkout: &std::path::Path) -> String {
        Repository::open(checkout.join("sub"))
            .unwrap()
            .head()
            .unwrap()
            .target()
            .unwrap()
            .to_string()
    }

    /// CoW hydration of a cache-shaped source with a submodule: a `base_ref`
    /// pinned at an older gitlink lands the submodule work tree on that older
    /// commit, resolved from the copied local module objects — no network
    /// (the submodule's only remote is deleted before provisioning). The
    /// source is untouched.
    #[test]
    fn cow_checkout_syncs_submodules_to_base_ref_gitlink_without_network() {
        crate::testutil::allow_file_submodules();
        let child = init_repo("cowchk-sub-child");
        commit_file(child.path(), "c.txt", "sub one\n");
        let old_sub_sha = head_sha(&child);
        let origin = init_repo("cowchk-sub-origin");
        commit_file(origin.path(), "a.txt", "one\n");
        crate::testutil::add_submodule(origin.path(), child.path(), "sub");
        // Pin `base` at the old gitlink, then bump the pin on the tip.
        crate::testutil::create_branch(origin.path(), "base");
        commit_file(child.path(), "c.txt", "sub two\n");
        let new_sub_sha = head_sha(&child);
        crate::testutil::commit_gitlink_bump(origin.path(), "sub", &new_sub_sha);

        // Cache-shaped source: recursive clone at the tip (submodule at the
        // NEW gitlink, module git dir holding the child's full history).
        let source = recursive_clone(origin.path(), "cowchk-sub-src");
        let _cleanup_src = Cleanup(source.clone());
        assert_eq!(sub_head(&source), new_sub_sha, "source sits at the tip");
        if !cow_available(&source) {
            return;
        }
        // Any fetch from the submodule's real URL now fails: the sync must
        // resolve from the copied module objects alone.
        drop(child);

        let checkout = unique_checkout("cowchk-sub-dst");
        let _cleanup = Cleanup(checkout.clone());
        provision_cow_checkout(&source, &checkout, "cow-ws", Some("base"), "origin", &[]).unwrap();

        assert_eq!(
            sub_head(&checkout),
            old_sub_sha,
            "submodule sits at the base ref's gitlink, not the source tip"
        );
        assert_eq!(
            std::fs::read_to_string(checkout.join("sub").join("c.txt")).unwrap(),
            "sub one\n"
        );
        // The source is untouched (still at the tip).
        assert_eq!(sub_head(&source), new_sub_sha);
        assert_eq!(
            std::fs::read_to_string(source.join("sub").join("c.txt")).unwrap(),
            "sub two\n"
        );
    }

    /// A submodule anomaly during CoW hydration (module git dirs gone AND the
    /// real URL unreachable) degrades to a warning — provisioning succeeds
    /// and the superproject checkout is intact.
    #[test]
    fn cow_checkout_submodule_failure_degrades_gracefully() {
        crate::testutil::allow_file_submodules();
        let child = init_repo("cowchk-subdeg-child");
        commit_file(child.path(), "c.txt", "sub one\n");
        let origin = init_repo("cowchk-subdeg-origin");
        commit_file(origin.path(), "a.txt", "one\n");
        crate::testutil::add_submodule(origin.path(), child.path(), "sub");

        let source = recursive_clone(origin.path(), "cowchk-subdeg-src");
        let _cleanup_src = Cleanup(source.clone());
        if !cow_available(&source) {
            return;
        }
        // Break the source's module git dirs and drop the only remote the
        // submodule could re-clone from.
        std::fs::remove_dir_all(source.join(".git").join("modules")).unwrap();
        drop(child);

        let checkout = unique_checkout("cowchk-subdeg-dst");
        let _cleanup = Cleanup(checkout.clone());
        let sha =
            provision_cow_checkout(&source, &checkout, "cow-ws", None, "origin", &[]).unwrap();

        assert!(!sha.is_empty(), "provisioning succeeds despite the anomaly");
        let clone = Repository::open(&checkout).unwrap();
        assert_eq!(clone.head().unwrap().shorthand().unwrap(), "cow-ws");
        assert_eq!(
            std::fs::read_to_string(checkout.join("a.txt")).unwrap(),
            "one\n",
            "superproject content is intact"
        );
    }

    /// A `base_ref` predating the submodule: the byte-copied work tree and
    /// its module git dir are orphans on the checked-out ref — both are
    /// removed and the superproject reports a clean status. The source keeps
    /// its populated submodule.
    #[test]
    fn cow_checkout_removes_orphaned_submodule_at_pre_submodule_base_ref() {
        crate::testutil::allow_file_submodules();
        let child = init_repo("cowchk-orph-child");
        commit_file(child.path(), "c.txt", "sub one\n");
        let origin = init_repo("cowchk-orph-origin");
        commit_file(origin.path(), "a.txt", "one\n");
        // Pin `base` before the submodule exists, then add it on the tip.
        crate::testutil::create_branch(origin.path(), "base");
        crate::testutil::add_submodule(origin.path(), child.path(), "sub");

        let source = recursive_clone(origin.path(), "cowchk-orph-src");
        let _cleanup_src = Cleanup(source.clone());
        if !cow_available(&source) {
            return;
        }

        let checkout = unique_checkout("cowchk-orph-dst");
        let _cleanup = Cleanup(checkout.clone());
        provision_cow_checkout(&source, &checkout, "cow-ws", Some("base"), "origin", &[]).unwrap();

        assert!(
            !checkout.join("sub").exists(),
            "orphaned submodule work tree is removed"
        );
        assert!(
            !checkout.join(".git").join("modules").join("sub").exists(),
            "dead module git dir is pruned"
        );
        let clone = Repository::open(&checkout).unwrap();
        let mut opts = git2::StatusOptions::new();
        opts.include_untracked(true).recurse_untracked_dirs(true);
        let statuses = clone.statuses(Some(&mut opts)).unwrap();
        let dirty: Vec<String> = statuses
            .iter()
            .filter_map(|s| s.path().map(str::to_owned).ok())
            .collect();
        assert!(dirty.is_empty(), "superproject status is clean: {dirty:?}");
        // The source keeps its populated submodule and module git dir.
        assert_eq!(
            std::fs::read_to_string(source.join("sub").join("c.txt")).unwrap(),
            "sub one\n"
        );
        assert!(source.join(".git").join("modules").join("sub").exists());
    }

    /// A nested submodule orphaned by the reset while its parent survives:
    /// `base` pins the parent gitlink at a revision without `inner`, the
    /// source tip carries `inner` populated. The nested work tree and its
    /// module git dir must both go, and the parent submodule reports a
    /// clean status (regression: only superproject-level gitlinks were
    /// captured, so `sub/inner` survived as an untracked nested repo with
    /// a dangling gitfile after its gitdir was pruned).
    #[test]
    fn cow_checkout_removes_nested_submodule_orphaned_by_parent_reset() {
        crate::testutil::allow_file_submodules();
        let grandchild = init_repo("cowchk-nestorph-grand");
        commit_file(grandchild.path(), "g.txt", "deep one\n");
        let child = init_repo("cowchk-nestorph-child");
        commit_file(child.path(), "c.txt", "sub one\n");
        let origin = init_repo("cowchk-nestorph-origin");
        commit_file(origin.path(), "a.txt", "one\n");
        crate::testutil::add_submodule(origin.path(), child.path(), "sub");
        // Pin `base` while `sub` has no nested module, then add `inner`
        // inside `sub` and bump the pin on the tip.
        crate::testutil::create_branch(origin.path(), "base");
        crate::testutil::add_submodule(child.path(), grandchild.path(), "inner");
        let new_sub_sha = head_sha(&child);
        crate::testutil::commit_gitlink_bump(origin.path(), "sub", &new_sub_sha);

        // Source at the tip: `sub` populated with `inner` inside it.
        let source = recursive_clone(origin.path(), "cowchk-nestorph-src");
        let _cleanup_src = Cleanup(source.clone());
        assert!(
            source.join("sub").join("inner").join("g.txt").exists(),
            "source carries the nested submodule populated"
        );
        if !cow_available(&source) {
            return;
        }

        let checkout = unique_checkout("cowchk-nestorph-dst");
        let _cleanup = Cleanup(checkout.clone());
        provision_cow_checkout(&source, &checkout, "cow-ws", Some("base"), "origin", &[]).unwrap();

        assert!(
            checkout.join("sub").join("c.txt").exists(),
            "parent submodule work tree survives the reset"
        );
        assert!(
            !checkout.join("sub").join("inner").exists(),
            "orphaned nested submodule work tree is removed"
        );
        assert!(
            !checkout
                .join(".git")
                .join("modules")
                .join("sub")
                .join("modules")
                .join("inner")
                .exists(),
            "dead nested module git dir is pruned"
        );
        // The parent submodule reports a clean status — no untracked
        // leftover from the removed nested repo.
        let sub = Repository::open(checkout.join("sub")).unwrap();
        let mut opts = git2::StatusOptions::new();
        opts.include_untracked(true).recurse_untracked_dirs(true);
        let statuses = sub.statuses(Some(&mut opts)).unwrap();
        let dirty: Vec<String> = statuses
            .iter()
            .filter_map(|s| s.path().map(str::to_owned).ok())
            .collect();
        assert!(
            dirty.is_empty(),
            "parent submodule status is clean: {dirty:?}"
        );
        // The source keeps its populated nested submodule and module dirs.
        assert_eq!(
            std::fs::read_to_string(source.join("sub").join("inner").join("g.txt")).unwrap(),
            "deep one\n"
        );
        assert!(source
            .join(".git")
            .join("modules")
            .join("sub")
            .join("modules")
            .join("inner")
            .exists());
    }

    /// After CoW hydration the checkout's `submodule.<name>.url` config
    /// matches `.gitmodules` even when the source carried a divergent
    /// configured URL — the closing `submodule sync` gives the CoW path the
    /// same URL parity as direct hydration.
    #[test]
    fn cow_checkout_syncs_divergent_submodule_url_to_gitmodules() {
        crate::testutil::allow_file_submodules();
        let child = init_repo("cowchk-url-child");
        commit_file(child.path(), "c.txt", "sub one\n");
        let origin = init_repo("cowchk-url-origin");
        commit_file(origin.path(), "a.txt", "one\n");
        crate::testutil::add_submodule(origin.path(), child.path(), "sub");

        let source = recursive_clone(origin.path(), "cowchk-url-src");
        let _cleanup_src = Cleanup(source.clone());
        if !cow_available(&source) {
            return;
        }
        // Diverge the source's configured URL from what `.gitmodules` says.
        Repository::open(&source)
            .unwrap()
            .config()
            .unwrap()
            .set_str("submodule.sub.url", "/nonexistent/divergent")
            .unwrap();

        let checkout = unique_checkout("cowchk-url-dst");
        let _cleanup = Cleanup(checkout.clone());
        provision_cow_checkout(&source, &checkout, "cow-ws", None, "origin", &[]).unwrap();

        let gitmodules_url = git2::Config::open(&checkout.join(".gitmodules"))
            .unwrap()
            .get_string("submodule.sub.url")
            .unwrap();
        let configured_url = Repository::open(&checkout)
            .unwrap()
            .config()
            .unwrap()
            .get_string("submodule.sub.url")
            .unwrap();
        assert_eq!(
            configured_url, gitmodules_url,
            "configured URL is re-pointed at the .gitmodules value"
        );
    }
}
