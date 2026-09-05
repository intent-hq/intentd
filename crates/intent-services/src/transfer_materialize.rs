//! Target-side git materialization for workspace transfer (spec §4, resolved
//! decision 1): at import-commit time, recreate the workspace checkout and
//! every transferred sandbox from the received bundle. The checkout is cloned
//! from the bundle at the workspace branch, every submodule the archive
//! bundled (unpublished commits, `git/submodules/<n>.bundle`) is hydrated
//! from its own bundle, the base ref is fetched as a local branch, sandboxes
//! are re-provisioned as `CoW` clones of the checkout (plain clone when `CoW`
//! is unavailable) with their `sb/<agentId>` branches fetched from the
//! bundle, and the sentinel WIP snapshot commits are unwound so
//! staged/unstaged/untracked state lands exactly as it was on the source.
//! All-or-nothing: any failure removes everything this module created. No
//! wire code lives here.

use std::path::{Path, PathBuf};
use std::process::Command;

use intent_core::{CheckoutMode, Error, Result, Workspace};
use intent_git::{cow_clone, cow_probe, CowSupport};
use intent_store::Sandbox;

use crate::transfer_git::{run_git, unwind_wip, SubmoduleBundleRef, TransferRefsManifest};

/// One sandbox as re-provisioned on the target.
#[derive(Debug, Clone)]
pub(crate) struct MaterializedSandbox {
    pub agent_id: String,
    /// Target-side sandbox directory
    /// (`<workspaces_root>/<wsId>/sandboxes/<agentId>/<repo-slug>`).
    pub path: PathBuf,
}

/// Result of a successful materialization: the target-side paths the caller
/// writes back onto the imported rows (see [`MaterializedGit::apply`]).
#[derive(Debug, Clone)]
pub(crate) struct MaterializedGit {
    /// The workspace checkout (`<workspaces_root>/<wsId>/<repo-slug>`),
    /// standalone (`CheckoutMode::Direct`) with only the source's portable
    /// remotes configured (`refs.remotes`) — the bundle was the only source
    /// and its staging path must not leak into the repo config.
    pub checkout_dir: PathBuf,
    /// Branch the checkout is on — the bundled `workspace_branch`, which is
    /// whatever HEAD pointed at when the bundle was built and may differ
    /// from the imported row's `branch`; [`MaterializedGit::apply`] rewrites
    /// the row to match.
    pub workspace_branch: String,
    /// The workspace base commit as recorded in the bundle manifest, for
    /// backfilling `Workspace.base_commit_sha` when the imported row has
    /// none.
    pub base_sha: Option<String>,
    /// Sandboxes re-provisioned on the target, in input order.
    pub sandboxes: Vec<MaterializedSandbox>,
    /// Agent ids of sandbox rows that had no branch in the bundle (skipped
    /// at bundle time — missing directory or unbundlable branch). Nothing
    /// was provisioned for them; [`MaterializedGit::apply`] drops their rows.
    /// Set by materialization; read by tests.
    #[cfg_attr(not(test), allow(dead_code))]
    pub skipped_agent_ids: Vec<String>,
}

impl MaterializedGit {
    /// Rewrite the imported rows against the materialized target paths: the
    /// workspace becomes a standalone Direct checkout at [`checkout_dir`]
    /// on [`workspace_branch`] (`base_commit_sha` backfilled from the bundle
    /// when missing), sandbox rows get their target paths, and rows whose
    /// branch never made it into the bundle are dropped — a row pointing at
    /// a directory that does not exist would present a broken sandbox.
    ///
    /// [`checkout_dir`]: MaterializedGit::checkout_dir
    /// [`workspace_branch`]: MaterializedGit::workspace_branch
    pub fn apply(&self, ws: &mut Workspace, sandboxes: &mut Vec<Sandbox>) {
        let checkout = self.checkout_dir.to_string_lossy().to_string();
        ws.repository_path = Some(checkout);
        ws.worktree_path = None;
        ws.checkout_mode = Some(CheckoutMode::Direct);
        ws.branch.clone_from(&self.workspace_branch);
        if ws.base_commit_sha.is_none() {
            ws.base_commit_sha.clone_from(&self.base_sha);
        }
        sandboxes.retain_mut(|sb| {
            match self.sandboxes.iter().find(|m| m.agent_id == sb.agent_id.0) {
                Some(m) => {
                    sb.path = m.path.to_string_lossy().to_string();
                    true
                }
                None => false,
            }
        });
    }
}

/// Async entry: run [`materialize_workspace_git_blocking`] on the blocking
/// pool. The materialized checkout is deliberately NOT registered in
/// `known_repo` — it lives under the workspaces root, i.e. daemon-managed
/// storage, and workspace-owned checkouts stay out of the registry
/// (intent-hq/monorepo#2227; supersedes the earlier transfer-spec decision
/// to register on target — the `repo.list` sync would sweep such a row
/// right back out).
pub(crate) async fn materialize_workspace_git(
    bundle_path: PathBuf,
    refs: TransferRefsManifest,
    ws: Workspace,
    sandboxes: Vec<Sandbox>,
    workspaces_root: PathBuf,
) -> Result<MaterializedGit> {
    tokio::task::spawn_blocking(move || {
        materialize_workspace_git_blocking(&bundle_path, &refs, &ws, &sandboxes, &workspaces_root)
    })
    .await
    .map_err(|e| Error::Internal(format!("materialize task failed: {e}")))?
}

/// Every directory a successful materialization created, in
/// [`rollback_created`] order: the checkout, then each sandbox's agent
/// directory.
fn created_paths(out: &MaterializedGit) -> Vec<PathBuf> {
    std::iter::once(out.checkout_dir.clone())
        .chain(out.sandboxes.iter().map(|s| {
            s.path
                .parent()
                .map_or_else(|| s.path.clone(), Path::to_path_buf)
        }))
        .collect()
}

/// Undo a SUCCESSFUL materialization after a later commit step fails (e.g.
/// the row insert): remove the created checkout/sandbox directories,
/// restoring the target exactly as found so the staged import can be
/// retried or aborted. Best-effort — the commit error being unwound takes
/// precedence.
pub(crate) fn rollback_materialized(out: &MaterializedGit, ws_dir: &Path) {
    rollback_created(&created_paths(out), ws_dir);
}

/// Materialize the workspace's git state from the transfer bundle. Blocking
/// work (git2 I/O plus `git` child processes); async callers must run it via
/// `spawn_blocking` — [`materialize_workspace_git`] does.
///
/// Sequence: clone the bundle at the workspace branch into
/// `<workspaces_root>/<wsId>/<repo-slug>` (origin removed — the staging
/// bundle path must not persist), hydrate every submodule listed in
/// `refs.submodules` from its own bundle (see [`hydrate_submodules`]), fetch
/// the base ref as a local branch, re-provision each sandbox as a `CoW`
/// clone of the checkout (plain local clone when `CoW` is unavailable) with
/// its branch fetched from the bundle and checked out, unwind the WIP
/// snapshot commits (sandboxes first, then the workspace) so the dirty state
/// lands exactly as captured, then restore the source's portable remotes,
/// remote-tracking refs and workspace-branch upstream from `refs.remotes`
/// (see `transfer_remotes`).
///
/// Submodule bundles are resolved next to `bundle_path` in the archive's
/// `git/` layout: entry `git/submodules/<n>.bundle` lives at
/// `<bundle_path dir>/submodules/<n>.bundle` — true both for an extracted
/// archive (`git/repo.bundle`) and for the export bundler's staging dir
/// (`<staging>/<wsId>.bundle`).
///
/// On failure every directory this call created is removed — the target is
/// restored exactly as found (no half-materialized workspace).
pub(crate) fn materialize_workspace_git_blocking(
    bundle_path: &Path,
    refs: &TransferRefsManifest,
    ws: &Workspace,
    sandboxes: &[Sandbox],
    workspaces_root: &Path,
) -> Result<MaterializedGit> {
    let ws_dir = workspaces_root.join(&ws.id.0);
    let mut created: Vec<PathBuf> = Vec::new();
    let result = materialize_inner(
        bundle_path,
        refs,
        ws,
        sandboxes,
        workspaces_root,
        &mut created,
    );
    if result.is_err() {
        rollback_created(&created, &ws_dir);
    }
    result
}

fn materialize_inner(
    bundle_path: &Path,
    refs: &TransferRefsManifest,
    ws: &Workspace,
    sandboxes: &[Sandbox],
    workspaces_root: &Path,
    created: &mut Vec<PathBuf>,
) -> Result<MaterializedGit> {
    let bundle = bundle_path
        .to_str()
        .ok_or_else(|| Error::Internal("bundle path not UTF-8".to_string()))?;
    if !bundle_path.is_file() {
        return Err(Error::Internal(format!(
            "transfer bundle missing: {}",
            bundle_path.display()
        )));
    }

    // Same naming as `workspace.create` provisioning: display name from the
    // row (falling back to the source path basename), slugified with
    // `worktree_folder_slug`. (Live sandbox re-provisioning slugifies with
    // `sandbox_ops::repo_slug_from_workspace`, which differs on edge cases —
    // intentional: rows store absolute paths, so a later re-provision naming
    // its folder differently is harmless, and the checkout and its sandboxes
    // sharing ONE slug matters more here.) `ws.repository_path` still holds
    // the SOURCE path — the caller rewrites it via [`MaterializedGit::apply`]
    // after success.
    let repo_name = crate::known_repo_name(
        ws.repository_name.as_deref(),
        ws.repository_path.as_deref().unwrap_or(""),
    );
    let ws_dir = workspaces_root.join(&ws.id.0);
    let checkout_dir = ws_dir.join(crate::worktree_folder_slug(&repo_name));
    if checkout_dir.exists() {
        return Err(Error::Internal(format!(
            "materialize target already exists: {}",
            checkout_dir.display()
        )));
    }
    std::fs::create_dir_all(&ws_dir)
        .map_err(|e| Error::Internal(format!("create workspace dir failed: {e}")))?;

    // 1. Clone the bundle at the workspace branch. The bundle is
    //    self-contained (full history, no prerequisites) so this needs no
    //    other remote; the whole pack lands in the object store, which is
    //    what makes the later base/sandbox ref fetches cheap.
    //    GIT_LFS_SKIP_SMUDGE: bundles carry LFS pointer blobs, not LFS
    //    objects, and this temporary origin is the bundle path — the smudge
    //    filter would fail on any target without a populated LFS cache.
    //    Skipping it leaves pointer files in the worktree; a later
    //    `git lfs pull` against the real remote hydrates them.
    created.push(checkout_dir.clone());
    run_git(&ws_dir, |cmd| {
        cmd.arg("clone")
            .arg("--quiet")
            .arg("-b")
            .arg(&refs.workspace_branch)
            .arg(bundle)
            .arg(&checkout_dir)
            .env("GIT_LFS_SKIP_SMUDGE", "1");
    })
    .map_err(|e| Error::Internal(format!("clone from transfer bundle failed: {e}")))?;

    // The clone's origin points at the staging bundle path, which is deleted
    // after commit — drop it so no dangling remote persists.
    run_git(&checkout_dir, |cmd| {
        cmd.arg("remote").arg("remove").arg("origin");
    })
    .map_err(|e| Error::Internal(format!("remove bundle origin failed: {e}")))?;

    // Sanity: the cloned tip must be exactly what the manifest recorded (the
    // WIP snapshot commit when the source was dirty).
    let cloned_head = head_sha(&checkout_dir)?;
    if cloned_head != refs.workspace_head_sha {
        return Err(Error::Internal(format!(
            "materialized checkout tip {cloned_head} does not match bundled head {}",
            refs.workspace_head_sha
        )));
    }

    // 1b. Hydrate the submodules whose commits rode the archive as their own
    //     bundles, while the checkout is still at the bundled tip (the
    //     gitlinks the WIP snapshot captured are what the bundles anchor).
    //     Everything lands under the checkout, so the rollback above covers
    //     a failure here.
    let bundle_dir = bundle_path
        .parent()
        .ok_or_else(|| Error::Internal("bundle path has no parent directory".to_string()))?;
    hydrate_submodules(&checkout_dir, bundle_dir, refs)?;

    // 2. Recreate the base ref as a local branch so base-relative operations
    //    (diffs, merge checks) resolve. Skipped when the base IS the
    //    workspace branch (already checked out) or when the bundle carries
    //    no base anchor.
    if let (Some(bundle_ref), Some(base_ref)) = (&refs.base_bundle_ref, &refs.base_ref) {
        if base_ref != &refs.workspace_branch {
            run_git(&checkout_dir, |cmd| {
                cmd.arg("fetch")
                    .arg("--no-tags")
                    .arg("--quiet")
                    .arg(bundle)
                    .arg(format!("+{bundle_ref}:refs/heads/{base_ref}"));
            })
            .map_err(|e| Error::Internal(format!("fetch base ref from bundle failed: {e}")))?;
        }
    }

    // 3. Re-provision each sandbox row that has a branch in the bundle,
    //    BEFORE unwinding the workspace WIP: sandboxes clone the checkout,
    //    and the checkout must still be clean at the bundled tip. Rows the
    //    bundler skipped (missing directory / unbundlable branch on the
    //    source) get nothing on disk; the caller drops them via
    //    [`MaterializedGit::apply`].
    let mut materialized = Vec::new();
    let mut skipped = Vec::new();
    for sb in sandboxes {
        let Some(entry) = refs.sandboxes.iter().find(|e| e.agent_id == sb.agent_id.0) else {
            tracing::warn!(
                agent = %sb.agent_id.0,
                branch = %sb.branch,
                "materialize: sandbox has no branch in the bundle; dropping the row"
            );
            skipped.push(sb.agent_id.0.clone());
            continue;
        };
        let agent_dir = ws_dir.join("sandboxes").join(&sb.agent_id.0);
        let sandbox_path = agent_dir.join(crate::worktree_folder_slug(&repo_name));
        // Only track for rollback what THIS call creates: a pre-existing
        // agent dir must never be recursively deleted by a later failure, so
        // in that case track just the sandbox path — which must not itself
        // pre-exist (same guard as the checkout dir above).
        if agent_dir.exists() {
            if sandbox_path.exists() {
                return Err(Error::Internal(format!(
                    "materialize sandbox target already exists: {}",
                    sandbox_path.display()
                )));
            }
            created.push(sandbox_path.clone());
        } else {
            created.push(agent_dir.clone());
        }
        provision_sandbox_from_bundle(
            &checkout_dir,
            bundle,
            &agent_dir,
            &sandbox_path,
            entry,
            &refs.workspace_branch,
            refs.workspace_wip_commit_sha.is_some(),
        )?;
        materialized.push(MaterializedSandbox {
            agent_id: sb.agent_id.0.clone(),
            path: sandbox_path,
        });
    }

    // 4. Unwind the workspace WIP snapshot, restoring the exact
    //    staged/unstaged/untracked split on the workspace branch.
    if refs.workspace_wip_commit_sha.is_some() && !unwind_wip(&checkout_dir)? {
        return Err(Error::Internal(
            "manifest records a workspace WIP snapshot but the checkout tip is not one".to_string(),
        ));
    }

    // 5. Reconnect the checkout to the source's portable remotes: recreate
    //    each remote, fetch its recorded tracking refs from the bundle, and
    //    restore the workspace branch's upstream — after the sandboxes were
    //    provisioned, so they keep the remote-less shape they have on the
    //    source, and after the bundle origin was removed, so `origin` can be
    //    recreated with its real URL.
    crate::transfer_remotes::restore_remotes(&checkout_dir, bundle, refs)?;

    Ok(MaterializedGit {
        checkout_dir,
        workspace_branch: refs.workspace_branch.clone(),
        base_sha: refs.base_sha.clone(),
        sandboxes: materialized,
        skipped_agent_ids: skipped,
    })
}

/// Hydrate every submodule listed in `refs.submodules` from its archive
/// bundle, in manifest order (parents before nested children, so a nested
/// entry's containing repository is already checked out). For each entry:
/// point `submodule.<name>.url` in the containing repository at the bundle,
/// `git submodule update --init` the path (clone + checkout of the gitlink
/// the containing tip records, which is the bundled `commit_sha`), recreate
/// the branch HEAD was on at that commit, then restore the source's
/// `remote.origin.url` (or remove the remote/url when the source had none)
/// so the staging bundle path never persists in any config. Empty
/// `submodules` (older archives, or every submodule published) is a no-op —
/// the checkout keeps its gitlinks uninitialized, as before.
///
/// A submodule whose containing repository is not checked out — a nested
/// entry whose published parent an older daemon did not bundle — is an
/// error: the commit cannot be placed, and silently dropping it would
/// contradict the export's "rides in the archive" promise. Current exports
/// bundle such parents (`published: true`) ahead of the nested entry.
fn hydrate_submodules(
    checkout_dir: &Path,
    bundle_dir: &Path,
    refs: &TransferRefsManifest,
) -> Result<()> {
    for sub in &refs.submodules {
        hydrate_submodule(checkout_dir, bundle_dir, sub)
            .map_err(|e| Error::Internal(format!("hydrate submodule {} failed: {e}", sub.path)))?;
        tracing::info!(
            path = %sub.path,
            commit = %sub.commit_sha,
            published = sub.published,
            "materialize: hydrated submodule from archive bundle"
        );
    }
    Ok(())
}

fn hydrate_submodule(
    checkout_dir: &Path,
    bundle_dir: &Path,
    sub: &SubmoduleBundleRef,
) -> std::result::Result<(), String> {
    if !is_full_sha(&sub.commit_sha) {
        return Err(format!(
            "manifest commit sha {:?} is not a full sha",
            sub.commit_sha
        ));
    }
    if sub.name.is_empty() || sub.name.contains(['\n', '\0']) {
        return Err(format!("manifest submodule name {:?} is invalid", sub.name));
    }
    check_relative_components(&sub.path).map_err(|e| format!("manifest path: {e}"))?;
    let bundle = submodule_bundle_path(bundle_dir, &sub.bundle_entry)?;
    if !bundle.is_file() {
        return Err(format!("bundle {} missing from archive", sub.bundle_entry));
    }
    let bundle = bundle
        .to_str()
        .ok_or_else(|| "bundle path not UTF-8".to_string())?;

    // The containing repository is the parent of the composed path — the
    // checkout itself for a top-level submodule, an already-hydrated
    // submodule for a nested one.
    let (parent_dir, rel) = match sub.path.rsplit_once('/') {
        Some((parent, rel)) => (checkout_dir.join(parent), rel),
        None => (checkout_dir.to_path_buf(), sub.path.as_str()),
    };
    if !parent_dir.join(".git").exists() {
        return Err(format!(
            "containing repository {} is not checked out (the archive did not bundle its published parent submodule; re-export with a current daemon)",
            parent_dir.display()
        ));
    }
    // `check_relative_components` rejects `..`, but the superproject tree is
    // user-supplied too: a committed symlink at any component of the path
    // would otherwise send the git calls below outside the checkout.
    let parent_dir = confined_to_checkout(checkout_dir, &parent_dir)?;

    // The gitlink at the containing tip must be the bundled commit: that is
    // what `submodule update` checks out, and what the export recorded.
    let gitlink = git_stdout(&parent_dir, |cmd| {
        cmd.args(["ls-tree", "HEAD", "--", rel]);
    })?;
    let mut fields = gitlink.split_whitespace();
    match (fields.next(), fields.next(), fields.next()) {
        (Some("160000"), Some("commit"), Some(sha)) if sha == sub.commit_sha => {}
        (Some("160000"), Some("commit"), Some(sha)) => {
            return Err(format!(
                "containing tip records gitlink {sha}, manifest bundled {}",
                sub.commit_sha
            ))
        }
        _ => return Err("path is not a submodule gitlink at the containing tip".to_string()),
    }

    // The manifest name must be the `.gitmodules` entry for this path at the
    // containing tip: `submodule update` resolves the path through that
    // entry, so a mismatched name would leave the bundle URL unused and
    // send the clone to the `.gitmodules` URL instead.
    let gitmodules_path = git_stdout(&parent_dir, |cmd| {
        cmd.args(["config", "--blob", "HEAD:.gitmodules", "--get"])
            .arg(format!("submodule.{}.path", sub.name));
    })
    .map_err(|e| {
        format!(
            "submodule.{}.path missing from .gitmodules at the containing tip: {e}",
            sub.name
        )
    })?;
    if gitmodules_path != rel {
        return Err(format!(
            ".gitmodules names submodule {:?} at path {gitmodules_path:?}, manifest places it at {rel:?}",
            sub.name
        ));
    }

    // Clone + check out from the bundle. The URL is preset so `init` keeps
    // it instead of copying `.gitmodules`; protocol.file.allow because the
    // bundle is a local path; GIT_LFS_SKIP_SMUDGE for the same reason as the
    // superproject clone.
    let module_dir = parent_dir.join(rel);
    if module_dir.symlink_metadata().is_ok() {
        confined_to_checkout(checkout_dir, &module_dir)?;
    }
    let url_key = format!("submodule.{}.url", sub.name);
    run_git(&parent_dir, |cmd| {
        cmd.args(["config", &url_key, bundle]);
    })
    .map_err(|e| format!("point submodule at bundle failed: {e}"))?;
    run_git(&parent_dir, |cmd| {
        cmd.args(["-c", "protocol.file.allow=always"])
            .args(["submodule", "update", "--init", "--quiet", "--", rel])
            .env("GIT_LFS_SKIP_SMUDGE", "1");
    })
    .map_err(|e| format!("submodule update from bundle failed: {e}"))?;

    let module_head = head_sha(&module_dir).map_err(|e| e.to_string())?;
    if module_head != sub.commit_sha {
        return Err(format!(
            "hydrated submodule HEAD {module_head} does not match bundled commit {}",
            sub.commit_sha
        ));
    }

    // Recreate the branch HEAD was on (the bundle carries no branch refs;
    // the clone lands detached at the commit).
    if let Some(branch) = &sub.branch {
        run_git(&module_dir, |cmd| {
            cmd.args(["checkout", "--quiet", "-B", branch, &sub.commit_sha])
                .env("GIT_LFS_SKIP_SMUDGE", "1");
        })
        .map_err(|e| format!("recreate branch {branch} failed: {e}"))?;
    }

    // Restore the source's origin URL everywhere the bundle path was written.
    if let Some(url) = &sub.origin_url {
        run_git(&parent_dir, |cmd| {
            cmd.args(["config", &url_key, url]);
        })
        .map_err(|e| format!("restore submodule url failed: {e}"))?;
        run_git(&module_dir, |cmd| {
            cmd.args(["remote", "set-url", "origin", url]);
        })
        .map_err(|e| format!("restore origin url failed: {e}"))?;
    } else {
        run_git(&parent_dir, |cmd| {
            cmd.args(["config", "--unset", &url_key]);
        })
        .map_err(|e| format!("unset submodule url failed: {e}"))?;
        run_git(&module_dir, |cmd| {
            cmd.args(["remote", "remove", "origin"]);
        })
        .map_err(|e| format!("remove bundle origin failed: {e}"))?;
    }
    Ok(())
}

/// Canonicalize `path` and require it to stay under the canonicalized
/// `checkout_dir` — the materialized tree may commit symlinks anywhere.
fn confined_to_checkout(checkout_dir: &Path, path: &Path) -> std::result::Result<PathBuf, String> {
    let root = checkout_dir
        .canonicalize()
        .map_err(|e| format!("canonicalize checkout {}: {e}", checkout_dir.display()))?;
    let resolved = path
        .canonicalize()
        .map_err(|e| format!("canonicalize {}: {e}", path.display()))?;
    if !resolved.starts_with(&root) {
        return Err(format!(
            "{} resolves to {} outside the checkout {}",
            path.display(),
            resolved.display(),
            root.display()
        ));
    }
    Ok(resolved)
}

/// Resolve an archive entry (`git/submodules/<n>.bundle`) to the file next
/// to the superproject bundle; rejects anything outside that layout.
fn submodule_bundle_path(bundle_dir: &Path, entry: &str) -> std::result::Result<PathBuf, String> {
    let rel = entry
        .strip_prefix("git/")
        .ok_or_else(|| format!("bundle entry {entry:?} is not under git/"))?;
    check_relative_components(rel).map_err(|e| format!("bundle entry {entry:?}: {e}"))?;
    Ok(bundle_dir.join(rel))
}

/// A forward-slash relative path with only plain components (no empty, `.`,
/// `..`, backslash or NUL) — the manifest is untrusted input.
fn check_relative_components(path: &str) -> std::result::Result<(), String> {
    if path.is_empty() {
        return Err("empty path".to_string());
    }
    for component in path.split('/') {
        if component.is_empty()
            || component == "."
            || component == ".."
            || component.contains(['\\', '\0'])
        {
            return Err(format!("unsafe path {path:?}"));
        }
    }
    Ok(())
}

fn is_full_sha(s: &str) -> bool {
    (s.len() == 40 || s.len() == 64) && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Run `git` in `dir` and return trimmed stdout; stderr on failure.
fn git_stdout(
    dir: &Path,
    configure: impl FnOnce(&mut Command),
) -> std::result::Result<String, String> {
    let mut cmd = Command::new("git");
    configure(&mut cmd);
    let out = cmd
        .current_dir(dir)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(std::process::Stdio::null())
        .output()
        .map_err(|e| e.to_string())?;
    if out.status.success() {
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
    }
}

/// Provision one sandbox: CoW-clone the (still clean) workspace checkout when
/// the filesystem supports it, else a plain local `git clone`; then fetch the
/// sandbox branch from the bundle, check it out, and unwind its WIP snapshot.
/// The sandbox's local copy of the workspace branch is reset off the WIP
/// sentinel (the clone happened while the checkout was still at the sentinel
/// tip; only the workspace checkout gets the later unwind).
#[allow(clippy::too_many_arguments)]
fn provision_sandbox_from_bundle(
    checkout_dir: &Path,
    bundle: &str,
    agent_dir: &Path,
    sandbox_path: &Path,
    entry: &crate::transfer_git::SandboxBundleRef,
    workspace_branch: &str,
    workspace_has_wip: bool,
) -> Result<()> {
    std::fs::create_dir_all(agent_dir)
        .map_err(|e| Error::Internal(format!("create sandbox parent dir failed: {e}")))?;

    // CoW first (same preference as source-side provisioning), degrading to
    // a plain local clone — hardlinked objects, so still cheap — when the
    // probe or the clone itself reports Unsupported. `sandbox_path` never
    // pre-exists (the caller rejects that), so the failure cleanup below can
    // only remove what this cow_clone partially created.
    let cow = matches!(
        cow_probe(checkout_dir, agent_dir),
        Ok(CowSupport::Supported)
    ) && match cow_clone(checkout_dir, sandbox_path) {
        Ok(()) => true,
        Err(e) => {
            if sandbox_path.exists() {
                let _ = std::fs::remove_dir_all(sandbox_path);
            }
            if matches!(e, Error::Unsupported(_)) {
                false
            } else {
                return Err(e);
            }
        }
    };
    if !cow {
        run_git(agent_dir, |cmd| {
            cmd.arg("clone")
                .arg("--quiet")
                .arg(checkout_dir)
                .arg(sandbox_path)
                .env("GIT_LFS_SKIP_SMUDGE", "1");
        })
        .map_err(|e| Error::Internal(format!("sandbox clone failed: {e}")))?;
        // Drop the origin pointing at the workspace checkout: sandboxes on
        // the source have no remote either (CoW copies of a checkout whose
        // remote is the user's repo, not a sibling path).
        let _ = run_git(sandbox_path, |cmd| {
            cmd.arg("remote").arg("remove").arg("origin");
        });
    }

    run_git(sandbox_path, |cmd| {
        cmd.arg("fetch")
            .arg("--no-tags")
            .arg("--quiet")
            .arg(bundle)
            .arg(format!("+{}:refs/heads/{}", entry.bundle_ref, entry.branch));
    })
    .map_err(|e| Error::Internal(format!("fetch sandbox branch from bundle failed: {e}")))?;
    run_git(sandbox_path, |cmd| {
        cmd.arg("checkout")
            .arg("--quiet")
            .arg(&entry.branch)
            .env("GIT_LFS_SKIP_SMUDGE", "1");
    })
    .map_err(|e| Error::Internal(format!("checkout sandbox branch failed: {e}")))?;

    // The clone was taken at the sentinel tip, so the sandbox's local
    // workspace branch points at the WIP snapshot commit — a tip that is
    // unreachable from canonical refs once the workspace unwinds (it would
    // trip `audit_diverged_sandbox_branches` on every merge-back). Reset it
    // to the sentinel's parent, the same tip the workspace unwind restores.
    if workspace_has_wip && workspace_branch != entry.branch {
        run_git(sandbox_path, |cmd| {
            cmd.arg("branch")
                .arg("--force")
                .arg(workspace_branch)
                .arg(format!("refs/heads/{workspace_branch}^"));
        })
        .map_err(|e| {
            Error::Internal(format!(
                "reset sandbox workspace branch off the WIP sentinel failed: {e}"
            ))
        })?;
    }

    let tip = head_sha(sandbox_path)?;
    if tip != entry.head_sha {
        return Err(Error::Internal(format!(
            "materialized sandbox tip {tip} does not match bundled head {}",
            entry.head_sha
        )));
    }
    if entry.wip_commit_sha.is_some() && !unwind_wip(sandbox_path)? {
        return Err(Error::Internal(
            "manifest records a sandbox WIP snapshot but the sandbox tip is not one".to_string(),
        ));
    }
    Ok(())
}

/// Remove everything a failed materialization created: each created
/// directory tree, then the (now possibly empty) `sandboxes` and workspace
/// directories. `remove_dir` refuses non-empty directories, so a workspace
/// dir holding anything else — e.g. one that existed before this call — is
/// never touched (same guarantee as `remove_workspace_dir_if_empty` on the
/// create path).
fn rollback_created(created: &[PathBuf], ws_dir: &Path) {
    for path in created.iter().rev() {
        if let Err(e) = std::fs::remove_dir_all(path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "materialize rollback: failed to remove created directory"
                );
            }
        }
    }
    let _ = std::fs::remove_dir(ws_dir.join("sandboxes"));
    let _ = std::fs::remove_dir(ws_dir);
}

/// HEAD commit SHA of a repository.
fn head_sha(repo_path: &Path) -> Result<String> {
    let repo = git2::Repository::open(repo_path)
        .map_err(|e| Error::Internal(format!("open materialized repo failed: {e}")))?;
    repo.head()
        .ok()
        .and_then(|h| h.target())
        .map(|oid| oid.to_string())
        .ok_or_else(|| Error::Internal("materialized repo has no HEAD commit".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transfer_git::{create_transfer_bundle, BranchUpstream, TransferBundle};
    use intent_core::{AgentId, WorkspaceId, WorkspaceStatus};
    use intent_store::SandboxStatus;
    use std::fs;

    fn now_iso() -> String {
        intent_core::now_iso()
    }

    fn init_repo(repo_path: &Path) {
        fs::create_dir_all(repo_path).unwrap();
        let repo = git2::Repository::init_opts(
            repo_path,
            git2::RepositoryInitOptions::new().initial_head("main"),
        )
        .unwrap();
        fs::write(repo_path.join("README.md"), "hello\n").unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        let tree_id = {
            let mut index = repo.index().unwrap();
            index.add_path(Path::new("README.md")).unwrap();
            index.write().unwrap();
            index.write_tree().unwrap()
        };
        let tree = repo.find_tree(tree_id).unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "Initial commit", &tree, &[])
            .unwrap();
    }

    fn commit_file(repo_path: &Path, file: &str, content: &str, message: &str) -> String {
        let repo = git2::Repository::open(repo_path).unwrap();
        fs::write(repo_path.join(file), content).unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(Path::new(file)).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = repo.find_tree(tree_oid).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        let parent = repo.head().unwrap().peel_to_commit().unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, message, &tree, &[&parent])
            .unwrap()
            .to_string()
    }

    fn repo_head(repo_path: &Path) -> String {
        head_sha(repo_path).unwrap()
    }

    fn head_branch(repo_path: &Path) -> String {
        let repo = git2::Repository::open(repo_path).unwrap();
        let name = repo.head().unwrap().shorthand().unwrap().to_string();
        name
    }

    /// Status entries as (path, staged, `wt_modified`, untracked) tuples,
    /// sorted, for exact source/target comparisons.
    fn status_fingerprint(repo_path: &Path) -> Vec<(String, bool, bool, bool)> {
        let repo = git2::Repository::open(repo_path).unwrap();
        let mut opts = git2::StatusOptions::new();
        opts.include_untracked(true)
            .recurse_untracked_dirs(true)
            .include_ignored(false);
        let statuses = repo.statuses(Some(&mut opts)).unwrap();
        let mut rows: Vec<_> = statuses
            .iter()
            .map(|e| {
                let s = e.status();
                (
                    e.path().unwrap_or_default().to_string(),
                    s.intersects(
                        git2::Status::INDEX_NEW
                            | git2::Status::INDEX_MODIFIED
                            | git2::Status::INDEX_DELETED,
                    ),
                    s.intersects(git2::Status::WT_MODIFIED | git2::Status::WT_DELETED),
                    s.contains(git2::Status::WT_NEW),
                )
            })
            .collect();
        rows.sort();
        rows
    }

    fn workspace_for_repo(repo_path: &Path) -> Workspace {
        let now = now_iso();
        Workspace {
            id: WorkspaceId::new(),
            title: "Test WS".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: intent_core::WorkspaceActivity::Idle,
            attention: intent_core::WorkspaceAttention::None,
            created_at: now.clone(),
            updated_at: now,
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: Some(repo_path.to_string_lossy().to_string()),
            repository_owner: None,
            repository_name: Some("test-repo".to_string()),
            worktree_path: None,
            scope: None,
            skip_worktree: true,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            context_links: None,
            archived: false,
            archived_at: None,
            pending_delete_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
        }
    }

    fn sandbox_row(ws: &Workspace, agent_id: &AgentId, path: &Path, branch: &str) -> Sandbox {
        let now = now_iso();
        Sandbox {
            id: format!("sb-{}", agent_id.0),
            workspace_id: ws.id.clone(),
            agent_id: agent_id.clone(),
            path: path.to_string_lossy().to_string(),
            branch: branch.to_string(),
            base_commit_sha: repo_head(path),
            snapshot_commit_sha: None,
            status: SandboxStatus::Created,
            retry_count: 0,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    /// Simulate a sandbox: clone the workspace repo and check out `sb/<agent>`.
    fn make_sandbox_clone(src: &Path, dst: &Path, branch: &str) {
        let out = Command::new("git")
            .arg("clone")
            .arg("--quiet")
            .arg(src)
            .arg(dst)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let repo = git2::Repository::open(dst).unwrap();
        let head = repo.head().unwrap().peel_to_commit().unwrap();
        repo.branch(branch, &head, false).unwrap();
        repo.set_head(&format!("refs/heads/{branch}")).unwrap();
    }

    fn remote_names(repo_path: &Path) -> Vec<String> {
        let repo = git2::Repository::open(repo_path).unwrap();
        let remotes = repo.remotes().unwrap();
        let mut names = Vec::new();
        for name in &remotes {
            if let Ok(Some(n)) = name {
                names.push(n.to_string());
            }
        }
        names
    }

    #[test]
    fn roundtrip_dirty_workspace_and_sandbox() {
        let src = tempfile::TempDir::new().unwrap();
        let repo = src.path().join("source-repo");
        init_repo(&repo);
        let base = repo_head(&repo);

        // Workspace branch `feature` off `main` with one committed change.
        {
            let r = git2::Repository::open(&repo).unwrap();
            let head = r.head().unwrap().peel_to_commit().unwrap();
            r.branch("feature", &head, false).unwrap();
            r.set_head("refs/heads/feature").unwrap();
        }
        let feature_tip = commit_file(&repo, "feature.txt", "feature\n", "feat: branch work");

        // Source dirty state: staged, unstaged, untracked.
        fs::write(repo.join("staged.txt"), "staged\n").unwrap();
        {
            let r = git2::Repository::open(&repo).unwrap();
            let mut index = r.index().unwrap();
            index.add_path(Path::new("staged.txt")).unwrap();
            index.write().unwrap();
        }
        fs::write(repo.join("README.md"), "modified\n").unwrap();
        fs::write(repo.join("untracked.txt"), "untracked\n").unwrap();
        let ws_fingerprint = status_fingerprint(&repo);

        let mut ws = workspace_for_repo(&repo);
        ws.branch = "feature".to_string();
        ws.base_ref = Some("main".to_string());

        // Dirty sandbox with a committed change on its branch.
        let agent = AgentId::new();
        let branch = format!("sb/{}", agent.0);
        let sb_src = src.path().join("sandbox");
        make_sandbox_clone(&repo, &sb_src, &branch);
        commit_file(&sb_src, "sb.txt", "sandbox work\n", "feat: sandbox commit");
        fs::write(sb_src.join("sb-wip.txt"), "sandbox wip\n").unwrap();
        let sb_fingerprint = status_fingerprint(&sb_src);
        let sb_committed_tip = {
            let r = git2::Repository::open(&sb_src).unwrap();
            let tip = r.head().unwrap().peel_to_commit().unwrap().id().to_string();
            tip
        };
        let sb = sandbox_row(&ws, &agent, &sb_src, &branch);

        let staging = src.path().join("staging");
        let TransferBundle {
            bundle_path, refs, ..
        } = create_transfer_bundle(&ws, std::slice::from_ref(&sb), &staging).unwrap();

        // Materialize into a fresh target root.
        let target = tempfile::TempDir::new().unwrap();
        let out = materialize_workspace_git_blocking(
            &bundle_path,
            &refs,
            &ws,
            std::slice::from_ref(&sb),
            target.path(),
        )
        .unwrap();

        // Workspace checkout: right path, branch, dirty state, no remotes.
        let expected_checkout = target.path().join(&ws.id.0).join("test-repo");
        assert_eq!(out.checkout_dir, expected_checkout);
        assert_eq!(head_branch(&out.checkout_dir), "feature");
        assert_eq!(
            repo_head(&out.checkout_dir),
            feature_tip,
            "WIP unwound to the branch tip"
        );
        assert_eq!(status_fingerprint(&out.checkout_dir), ws_fingerprint);
        assert_eq!(
            fs::read_to_string(out.checkout_dir.join("untracked.txt")).unwrap(),
            "untracked\n"
        );
        assert!(remote_names(&out.checkout_dir).is_empty(), "no remotes");

        // Base branch materialized locally.
        {
            let r = git2::Repository::open(&out.checkout_dir).unwrap();
            let tip = r
                .find_reference("refs/heads/main")
                .unwrap()
                .peel_to_commit()
                .unwrap();
            assert_eq!(tip.id().to_string(), base);
        }
        assert_eq!(out.base_sha.as_deref(), Some(base.as_str()));

        // Sandbox: right path, branch checked out, dirty state restored.
        assert_eq!(out.sandboxes.len(), 1);
        let msb = &out.sandboxes[0];
        assert_eq!(
            msb.path,
            target
                .path()
                .join(&ws.id.0)
                .join("sandboxes")
                .join(&agent.0)
                .join("test-repo")
        );
        assert_eq!(head_branch(&msb.path), branch);
        assert_eq!(repo_head(&msb.path), sb_committed_tip, "WIP unwound");
        assert_eq!(status_fingerprint(&msb.path), sb_fingerprint);
        assert_eq!(
            fs::read_to_string(msb.path.join("sb.txt")).unwrap(),
            "sandbox work\n"
        );
        assert_eq!(
            fs::read_to_string(msb.path.join("sb-wip.txt")).unwrap(),
            "sandbox wip\n"
        );
        assert!(out.skipped_agent_ids.is_empty());

        // The sandbox's local workspace branch must not point at the WIP
        // sentinel (the clone was taken at the sentinel tip; it gets reset
        // to the unwound tip).
        {
            let r = git2::Repository::open(&msb.path).unwrap();
            let ws_branch_tip = r
                .find_reference("refs/heads/feature")
                .unwrap()
                .peel_to_commit()
                .unwrap();
            assert_eq!(
                ws_branch_tip.id().to_string(),
                feature_tip,
                "sandbox workspace branch reset off the WIP sentinel"
            );
        }

        // apply(): rows rewritten to target paths.
        let mut ws_row = ws.clone();
        let mut sb_rows = vec![sb];
        out.apply(&mut ws_row, &mut sb_rows);
        assert_eq!(
            ws_row.repository_path.as_deref(),
            Some(expected_checkout.to_string_lossy().as_ref())
        );
        assert_eq!(ws_row.worktree_path, None);
        assert_eq!(ws_row.checkout_mode, Some(CheckoutMode::Direct));
        assert_eq!(ws_row.base_commit_sha.as_deref(), Some(base.as_str()));
        assert_eq!(sb_rows.len(), 1);
        assert_eq!(sb_rows[0].path, msb.path.to_string_lossy());
    }

    #[test]
    fn clean_workspace_materializes_without_wip() {
        let src = tempfile::TempDir::new().unwrap();
        let repo = src.path().join("source-repo");
        init_repo(&repo);
        let tip = repo_head(&repo);
        let ws = workspace_for_repo(&repo);

        let TransferBundle {
            bundle_path, refs, ..
        } = create_transfer_bundle(&ws, &[], &src.path().join("staging")).unwrap();
        assert_eq!(refs.workspace_wip_commit_sha, None);

        let target = tempfile::TempDir::new().unwrap();
        let out = materialize_workspace_git_blocking(&bundle_path, &refs, &ws, &[], target.path())
            .unwrap();
        assert_eq!(repo_head(&out.checkout_dir), tip);
        assert!(status_fingerprint(&out.checkout_dir).is_empty(), "clean");
    }

    #[test]
    fn sandbox_row_without_bundle_entry_is_dropped() {
        let src = tempfile::TempDir::new().unwrap();
        let repo = src.path().join("source-repo");
        init_repo(&repo);
        let ws = workspace_for_repo(&repo);

        // Bundle carries no sandbox branches...
        let TransferBundle {
            bundle_path, refs, ..
        } = create_transfer_bundle(&ws, &[], &src.path().join("staging")).unwrap();

        // ...but a sandbox row rides in the import anyway.
        let agent = AgentId::new();
        let sb = sandbox_row(&ws, &agent, &repo, &format!("sb/{}", agent.0));

        let target = tempfile::TempDir::new().unwrap();
        let out = materialize_workspace_git_blocking(
            &bundle_path,
            &refs,
            &ws,
            std::slice::from_ref(&sb),
            target.path(),
        )
        .unwrap();
        assert!(out.sandboxes.is_empty());
        assert_eq!(out.skipped_agent_ids, vec![agent.0.clone()]);

        let mut ws_row = ws.clone();
        let mut sb_rows = vec![sb];
        out.apply(&mut ws_row, &mut sb_rows);
        assert!(sb_rows.is_empty(), "row without a directory is dropped");
        assert!(!target.path().join(&ws.id.0).join("sandboxes").exists());
    }

    #[test]
    fn failure_rolls_back_created_directories() {
        let src = tempfile::TempDir::new().unwrap();
        let repo = src.path().join("source-repo");
        init_repo(&repo);
        let ws = workspace_for_repo(&repo);

        let TransferBundle {
            bundle_path,
            mut refs,
            ..
        } = create_transfer_bundle(&ws, &[], &src.path().join("staging")).unwrap();
        // Corrupt the manifest so the post-clone tip check fails AFTER the
        // checkout directory has been created.
        refs.workspace_head_sha = "0".repeat(40);

        let target = tempfile::TempDir::new().unwrap();
        let err = materialize_workspace_git_blocking(&bundle_path, &refs, &ws, &[], target.path());
        assert!(err.is_err());
        assert!(
            !target.path().join(&ws.id.0).exists(),
            "workspace dir rolled back"
        );
    }

    #[test]
    fn missing_bundle_fails_cleanly() {
        let src = tempfile::TempDir::new().unwrap();
        let repo = src.path().join("source-repo");
        init_repo(&repo);
        let ws = workspace_for_repo(&repo);
        let TransferBundle {
            bundle_path, refs, ..
        } = create_transfer_bundle(&ws, &[], &src.path().join("staging")).unwrap();
        fs::remove_file(&bundle_path).unwrap();

        let target = tempfile::TempDir::new().unwrap();
        let err = materialize_workspace_git_blocking(&bundle_path, &refs, &ws, &[], target.path());
        assert!(err.is_err());
        assert!(!target.path().join(&ws.id.0).exists());
    }

    #[test]
    fn existing_checkout_target_is_rejected_and_untouched() {
        let src = tempfile::TempDir::new().unwrap();
        let repo = src.path().join("source-repo");
        init_repo(&repo);
        let ws = workspace_for_repo(&repo);
        let TransferBundle {
            bundle_path, refs, ..
        } = create_transfer_bundle(&ws, &[], &src.path().join("staging")).unwrap();

        let target = tempfile::TempDir::new().unwrap();
        let existing = target.path().join(&ws.id.0).join("test-repo");
        fs::create_dir_all(&existing).unwrap();
        fs::write(existing.join("keep.txt"), "keep\n").unwrap();

        let err = materialize_workspace_git_blocking(&bundle_path, &refs, &ws, &[], target.path());
        assert!(err.is_err());
        assert_eq!(
            fs::read_to_string(existing.join("keep.txt")).unwrap(),
            "keep\n",
            "pre-existing directory untouched"
        );
    }

    /// A pre-existing `sandboxes/<agentId>` directory must survive rollback:
    /// only what THIS call creates inside it (the sandbox checkout) may be
    /// removed on failure, and a pre-existing sandbox checkout path is
    /// rejected outright.
    #[test]
    fn rollback_preserves_preexisting_agent_dir() {
        let src = tempfile::TempDir::new().unwrap();
        let repo = src.path().join("source-repo");
        init_repo(&repo);
        let ws = workspace_for_repo(&repo);

        let agent = AgentId::new();
        let branch = format!("sb/{}", agent.0);
        let sb_src = src.path().join("sandbox");
        make_sandbox_clone(&repo, &sb_src, &branch);
        let sb = sandbox_row(&ws, &agent, &sb_src, &branch);

        let staging = src.path().join("staging");
        let TransferBundle {
            bundle_path,
            mut refs,
            ..
        } = create_transfer_bundle(&ws, std::slice::from_ref(&sb), &staging).unwrap();

        // Pre-existing agent dir with unrelated content on the target.
        let target = tempfile::TempDir::new().unwrap();
        let agent_dir = target
            .path()
            .join(&ws.id.0)
            .join("sandboxes")
            .join(&agent.0);
        fs::create_dir_all(&agent_dir).unwrap();
        fs::write(agent_dir.join("precious.txt"), "precious\n").unwrap();

        // Force a failure AFTER the sandbox is provisioned: corrupt the
        // sandbox head so its tip check fails.
        refs.sandboxes[0].head_sha = "0".repeat(40);

        let err = materialize_workspace_git_blocking(
            &bundle_path,
            &refs,
            &ws,
            std::slice::from_ref(&sb),
            target.path(),
        );
        assert!(err.is_err());
        assert_eq!(
            fs::read_to_string(agent_dir.join("precious.txt")).unwrap(),
            "precious\n",
            "pre-existing agent dir content survives rollback"
        );
        assert!(
            !agent_dir.join("test-repo").exists(),
            "the sandbox checkout this call created was rolled back"
        );

        // A pre-existing sandbox checkout path is rejected without touching it.
        let sandbox_checkout = agent_dir.join("test-repo");
        fs::create_dir_all(&sandbox_checkout).unwrap();
        fs::write(sandbox_checkout.join("keep.txt"), "keep\n").unwrap();
        let err = materialize_workspace_git_blocking(
            &bundle_path,
            &refs,
            &ws,
            std::slice::from_ref(&sb),
            target.path(),
        );
        assert!(err.is_err());
        assert_eq!(
            fs::read_to_string(sandbox_checkout.join("keep.txt")).unwrap(),
            "keep\n",
            "pre-existing sandbox checkout untouched"
        );
    }

    // -- submodule hydration ------------------------------------------------

    use crate::transfer_submodules::test_fixture::{
        git as fgit, init_repo as finit_repo, local_commit,
        nested_unpublished_under_published_parent as nested_fixture, superproject_with_submodule,
        NestedFixture,
    };

    /// Every git config file under `.git` (the repo's own plus each
    /// `.git/modules/**/config`) must be free of `needle` — the staging
    /// bundle path must never persist.
    fn assert_no_config_mentions(git_dir: &Path, needle: &str) {
        fn walk(dir: &Path, needle: &str) {
            for entry in fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    walk(&path, needle);
                } else if path.file_name().is_some_and(|n| n == "config") {
                    let text = fs::read_to_string(&path).unwrap();
                    assert!(
                        !text.contains(needle),
                        "{} still references {needle}:\n{text}",
                        path.display()
                    );
                }
            }
        }
        walk(git_dir, needle);
    }

    fn superproject_workspace(sup: &Path) -> Workspace {
        let mut ws = workspace_for_repo(sup);
        ws.repository_name = Some("super".to_string());
        ws
    }

    /// (a) An unpublished submodule commit round-trips: the target's
    /// submodule is initialized at the bundled commit on its original
    /// branch with the original origin URL, the superproject status matches
    /// the source (gitlink modified, WIP unwound), and no config anywhere
    /// mentions the staging bundle.
    #[test]
    fn roundtrip_hydrates_unpublished_submodule() {
        let tmp = tempfile::tempdir().unwrap();
        let (sup, origin) = superproject_with_submodule(tmp.path());
        let sub = sup.join("sub");
        fgit(&sub, &["checkout", "-q", "main"]);
        let sha = local_commit(&sub, "wip.txt");
        let src_status = fgit(&sup, &["status", "--porcelain"]);
        assert_eq!(
            src_status, "M sub",
            "source has a modified gitlink (trimmed)"
        );

        let ws = superproject_workspace(&sup);
        let staging = tmp.path().join("staging");
        let TransferBundle {
            bundle_path,
            refs,
            submodule_bundles,
        } = create_transfer_bundle(&ws, &[], &staging).unwrap();
        assert_eq!(refs.submodules.len(), 1, "{refs:?}");
        assert_eq!(submodule_bundles[0].0, staging.join("submodules/0.bundle"));

        let target = tempfile::tempdir().unwrap();
        let out = materialize_workspace_git_blocking(&bundle_path, &refs, &ws, &[], target.path())
            .unwrap();
        let dst_sub = out.checkout_dir.join("sub");

        assert!(dst_sub.join(".git").exists(), "submodule initialized");
        assert_eq!(repo_head(&dst_sub), sha);
        assert_eq!(head_branch(&dst_sub), "main");
        assert_eq!(
            fs::read_to_string(dst_sub.join("wip.txt")).unwrap(),
            "wip.txt\n"
        );
        assert_eq!(
            fgit(&dst_sub, &["remote", "get-url", "origin"]),
            origin.to_str().unwrap()
        );
        assert_eq!(
            fgit(&out.checkout_dir, &["config", "submodule.sub.url"]),
            origin.to_str().unwrap()
        );
        assert_eq!(
            fgit(&out.checkout_dir, &["status", "--porcelain"]),
            src_status
        );
        assert!(
            fgit(&out.checkout_dir, &["submodule", "status"]).starts_with(&format!("+{sha}")),
            "gitlink differs from HEAD exactly as on the source"
        );
        assert_no_config_mentions(&out.checkout_dir.join(".git"), staging.to_str().unwrap());
        assert_eq!(remote_names(&out.checkout_dir), Vec::<String>::new());
    }

    /// A submodule whose removal is staged (`git rm --cached sub`, checkout
    /// left on disk) with an unpublished local commit is NOT bundled: the WIP
    /// tip carries the removal and has no gitlink to hydrate against, so
    /// bundling it would make the import roll back. The archive round-trips
    /// exactly as before submodule bundling existed.
    #[test]
    fn staged_submodule_removal_is_not_bundled_and_roundtrips() {
        let tmp = tempfile::tempdir().unwrap();
        let (sup, _origin) = superproject_with_submodule(tmp.path());
        let sub = sup.join("sub");
        fgit(&sub, &["checkout", "-q", "main"]);
        local_commit(&sub, "wip.txt");
        fgit(&sup, &["rm", "-q", "--cached", "sub"]);
        let src_status = fgit(&sup, &["status", "--porcelain"]);
        assert_eq!(src_status, "D  sub\n?? sub/");

        let ws = superproject_workspace(&sup);
        let staging = tmp.path().join("staging");
        let TransferBundle {
            bundle_path,
            refs,
            submodule_bundles,
        } = create_transfer_bundle(&ws, &[], &staging).unwrap();
        assert!(refs.submodules.is_empty(), "{refs:?}");
        assert!(submodule_bundles.is_empty());

        let target = tempfile::tempdir().unwrap();
        let out = materialize_workspace_git_blocking(&bundle_path, &refs, &ws, &[], target.path())
            .unwrap();
        assert_eq!(
            fgit(&out.checkout_dir, &["status", "--porcelain"]),
            "D  sub",
            "staged removal travels; the on-disk checkout does not"
        );
        assert!(!out.checkout_dir.join("sub").exists());
    }

    /// A nested submodule whose checkout HEAD is NOT the gitlink its parent's
    /// HEAD records (committed in `sub/inner`, not yet committed in `sub`)
    /// cannot be placed by hydration — `sub` is bundled as-is, never
    /// snapshotted — so it is skipped rather than bundled at a commit the
    /// containing tip does not record; the import succeeds with `sub`
    /// hydrated at the tip it records.
    #[test]
    fn nested_submodule_unrecorded_by_parent_head_is_not_bundled() {
        let tmp = tempfile::tempdir().unwrap();
        let inner_src = tmp.path().join("inner-src");
        finit_repo(&inner_src);
        fgit(
            tmp.path(),
            &["clone", "-q", "--bare", "inner-src", "inner.git"],
        );
        let inner_origin = tmp.path().join("inner.git");
        let (sup, _sub_origin) = superproject_with_submodule(tmp.path());
        let sub = sup.join("sub");
        fgit(&sub, &["checkout", "-q", "main"]);
        fgit(
            &sub,
            &[
                "submodule",
                "add",
                "-q",
                inner_origin.to_str().unwrap(),
                "inner",
            ],
        );
        fgit(&sub, &["commit", "-q", "-m", "add inner"]);
        let sub_sha = fgit(&sub, &["rev-parse", "HEAD"]);
        let inner = sub.join("inner");
        let recorded_inner = fgit(&inner, &["rev-parse", "HEAD"]);
        fgit(&inner, &["checkout", "-q", "-b", "feat/x"]);
        local_commit(&inner, "deep.txt");

        let ws = superproject_workspace(&sup);
        let staging = tmp.path().join("staging");
        let TransferBundle {
            bundle_path, refs, ..
        } = create_transfer_bundle(&ws, &[], &staging).unwrap();
        let paths: Vec<&str> = refs.submodules.iter().map(|s| s.path.as_str()).collect();
        assert_eq!(paths, ["sub"], "{refs:?}");

        let target = tempfile::tempdir().unwrap();
        let out = materialize_workspace_git_blocking(&bundle_path, &refs, &ws, &[], target.path())
            .unwrap();
        let dst_sub = out.checkout_dir.join("sub");
        assert_eq!(repo_head(&dst_sub), sub_sha);
        assert_eq!(
            fgit(&dst_sub, &["ls-tree", "HEAD", "--", "inner"]),
            format!("160000 commit {recorded_inner}\tinner")
        );
        assert!(
            !dst_sub.join("inner/.git").exists(),
            "inner stays an uninitialized gitlink"
        );
    }

    /// (b) Nested: `sub` (unpublished — it records a bumped `inner`
    /// gitlink locally) and `sub/inner` (unpublished, on `feat/x`) both
    /// hydrate, parent first, each at its bundled commit and branch.
    #[test]
    fn roundtrip_hydrates_nested_submodules() {
        let tmp = tempfile::tempdir().unwrap();
        let inner_src = tmp.path().join("inner-src");
        finit_repo(&inner_src);
        fgit(
            tmp.path(),
            &["clone", "-q", "--bare", "inner-src", "inner.git"],
        );
        let inner_origin = tmp.path().join("inner.git");
        let (sup, sub_origin) = superproject_with_submodule(tmp.path());
        let sub = sup.join("sub");
        fgit(&sub, &["checkout", "-q", "main"]);
        fgit(
            &sub,
            &[
                "submodule",
                "add",
                "-q",
                inner_origin.to_str().unwrap(),
                "inner",
            ],
        );
        fgit(&sub, &["commit", "-q", "-m", "add inner"]);
        fgit(&sub, &["push", "-q", "origin", "main"]);

        let inner = sub.join("inner");
        fgit(&inner, &["checkout", "-q", "-b", "feat/x"]);
        let inner_sha = local_commit(&inner, "deep.txt");
        // `sub` records the new inner gitlink in a commit that is never pushed.
        fgit(&sub, &["add", "inner"]);
        fgit(&sub, &["commit", "-q", "-m", "bump inner"]);
        let sub_sha = fgit(&sub, &["rev-parse", "HEAD"]);
        // Captured before bundling: the bundler leaves its WIP snapshot on
        // the source until the export settles.
        let src_status = fgit(&sup, &["status", "--porcelain"]);
        assert_eq!(src_status, "M sub");

        let ws = superproject_workspace(&sup);
        let staging = tmp.path().join("staging");
        let TransferBundle {
            bundle_path, refs, ..
        } = create_transfer_bundle(&ws, &[], &staging).unwrap();
        let paths: Vec<&str> = refs.submodules.iter().map(|s| s.path.as_str()).collect();
        assert_eq!(paths, ["sub", "sub/inner"], "{refs:?}");

        let target = tempfile::tempdir().unwrap();
        let out = materialize_workspace_git_blocking(&bundle_path, &refs, &ws, &[], target.path())
            .unwrap();
        let dst_sub = out.checkout_dir.join("sub");
        let dst_inner = dst_sub.join("inner");

        assert_eq!(repo_head(&dst_sub), sub_sha);
        assert_eq!(head_branch(&dst_sub), "main");
        assert_eq!(
            fgit(&dst_sub, &["remote", "get-url", "origin"]),
            sub_origin.to_str().unwrap()
        );
        assert_eq!(repo_head(&dst_inner), inner_sha);
        assert_eq!(head_branch(&dst_inner), "feat/x");
        assert_eq!(
            fs::read_to_string(dst_inner.join("deep.txt")).unwrap(),
            "deep.txt\n"
        );
        assert_eq!(
            fgit(&dst_inner, &["remote", "get-url", "origin"]),
            inner_origin.to_str().unwrap()
        );
        assert_eq!(
            fgit(&dst_sub, &["config", "submodule.inner.url"]),
            inner_origin.to_str().unwrap()
        );
        assert_eq!(
            fgit(&out.checkout_dir, &["status", "--porcelain"]),
            src_status
        );
        assert_no_config_mentions(&out.checkout_dir.join(".git"), staging.to_str().unwrap());
    }

    /// A sandbox still re-provisions (`CoW` or plain clone of the checkout)
    /// after the checkout's submodule was hydrated: the sandbox lands on its
    /// branch at its bundled tip with its WIP unwound, the workspace
    /// checkout keeps the hydrated submodule, and no config under the
    /// workspace dir mentions the staging bundle.
    #[test]
    fn sandbox_provisions_after_submodule_hydration() {
        let tmp = tempfile::tempdir().unwrap();
        let (sup, origin) = superproject_with_submodule(tmp.path());
        let sub = sup.join("sub");
        fgit(&sub, &["checkout", "-q", "main"]);
        let sha = local_commit(&sub, "wip.txt");

        let ws = superproject_workspace(&sup);
        let agent = AgentId::new();
        let branch = format!("sb/{}", agent.0);
        let sb_src = tmp.path().join("sandbox");
        make_sandbox_clone(&sup, &sb_src, &branch);
        let sb_tip = commit_file(&sb_src, "sb.txt", "sandbox work\n", "feat: sandbox commit");
        fs::write(sb_src.join("sb-wip.txt"), "sandbox wip\n").unwrap();
        let sb_fingerprint = status_fingerprint(&sb_src);
        let sb = sandbox_row(&ws, &agent, &sb_src, &branch);

        let staging = tmp.path().join("staging");
        let TransferBundle {
            bundle_path, refs, ..
        } = create_transfer_bundle(&ws, std::slice::from_ref(&sb), &staging).unwrap();
        assert_eq!(refs.submodules.len(), 1, "{refs:?}");

        let target = tempfile::tempdir().unwrap();
        let out = materialize_workspace_git_blocking(
            &bundle_path,
            &refs,
            &ws,
            std::slice::from_ref(&sb),
            target.path(),
        )
        .unwrap();

        assert_eq!(repo_head(&out.checkout_dir.join("sub")), sha);
        assert_eq!(
            fgit(&out.checkout_dir, &["config", "submodule.sub.url"]),
            origin.to_str().unwrap()
        );
        assert_eq!(out.sandboxes.len(), 1);
        let msb = &out.sandboxes[0];
        assert_eq!(head_branch(&msb.path), branch);
        assert_eq!(repo_head(&msb.path), sb_tip, "WIP unwound");
        assert_eq!(status_fingerprint(&msb.path), sb_fingerprint);
        assert_eq!(
            fs::read_to_string(msb.path.join("sb-wip.txt")).unwrap(),
            "sandbox wip\n"
        );
        assert_no_config_mentions(&target.path().join(&ws.id.0), staging.to_str().unwrap());
    }

    /// (c) A corrupt submodule bundle fails the materialization with an
    /// error naming the submodule path, and the rollback leaves nothing
    /// behind under the target root.
    #[test]
    fn corrupt_submodule_bundle_fails_and_rolls_back() {
        let tmp = tempfile::tempdir().unwrap();
        let (sup, _origin) = superproject_with_submodule(tmp.path());
        let sub = sup.join("sub");
        fgit(&sub, &["checkout", "-q", "main"]);
        local_commit(&sub, "wip.txt");

        let ws = superproject_workspace(&sup);
        let staging = tmp.path().join("staging");
        let TransferBundle {
            bundle_path,
            refs,
            submodule_bundles,
        } = create_transfer_bundle(&ws, &[], &staging).unwrap();
        fs::write(&submodule_bundles[0].0, b"not a bundle").unwrap();

        let target = tempfile::tempdir().unwrap();
        let err = materialize_workspace_git_blocking(&bundle_path, &refs, &ws, &[], target.path())
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("hydrate submodule sub failed"), "got {msg}");
        assert!(
            !target.path().join(&ws.id.0).exists(),
            "rollback removed the workspace dir"
        );
    }

    /// (b') Nested under a PUBLISHED parent: `sub` (pushed) is carried
    /// `published: true` ahead of `sub/inner` (unpublished, `feat/x`); both
    /// hydrate from the archive with no network — the parent at its pushed
    /// commit on `main`, the child at the local-only commit — and the
    /// target matches the source's status with no staging path persisted.
    #[test]
    fn roundtrip_hydrates_nested_submodule_under_published_parent() {
        let tmp = tempfile::tempdir().unwrap();
        let NestedFixture {
            sup,
            sub_origin,
            inner_origin,
            sub_sha,
            inner_sha,
        } = nested_fixture(tmp.path());
        let src_status = fgit(&sup, &["status", "--porcelain"]);
        assert_eq!(src_status, "M sub");

        let ws = superproject_workspace(&sup);
        let staging = tmp.path().join("staging");
        let TransferBundle {
            bundle_path, refs, ..
        } = create_transfer_bundle(&ws, &[], &staging).unwrap();
        let entries: Vec<(&str, bool)> = refs
            .submodules
            .iter()
            .map(|s| (s.path.as_str(), s.published))
            .collect();
        assert_eq!(entries, [("sub", true), ("sub/inner", false)], "{refs:?}");

        // No network: the origins are unreachable while materializing.
        let sub_origin_hidden = tmp.path().join("sub-origin.hidden");
        let inner_origin_hidden = tmp.path().join("inner-origin.hidden");
        fs::rename(&sub_origin, &sub_origin_hidden).unwrap();
        fs::rename(&inner_origin, &inner_origin_hidden).unwrap();
        let target = tempfile::tempdir().unwrap();
        let out = materialize_workspace_git_blocking(&bundle_path, &refs, &ws, &[], target.path())
            .unwrap();
        fs::rename(&sub_origin_hidden, &sub_origin).unwrap();
        fs::rename(&inner_origin_hidden, &inner_origin).unwrap();

        let dst_sub = out.checkout_dir.join("sub");
        let dst_inner = dst_sub.join("inner");
        assert_eq!(repo_head(&dst_sub), sub_sha);
        assert_eq!(head_branch(&dst_sub), "main");
        assert_eq!(
            fgit(&dst_sub, &["remote", "get-url", "origin"]),
            sub_origin.to_str().unwrap()
        );
        assert_eq!(repo_head(&dst_inner), inner_sha);
        assert_eq!(head_branch(&dst_inner), "feat/x");
        assert_eq!(
            fs::read_to_string(dst_inner.join("deep.txt")).unwrap(),
            "deep.txt\n"
        );
        assert_eq!(
            fgit(&dst_inner, &["remote", "get-url", "origin"]),
            inner_origin.to_str().unwrap()
        );
        assert_eq!(
            fgit(&out.checkout_dir, &["status", "--porcelain"]),
            src_status
        );
        assert_no_config_mentions(&out.checkout_dir.join(".git"), staging.to_str().unwrap());
    }

    /// An archive from a daemon that did not carry the published parent
    /// (only `sub/inner` in `refs.submodules`) is rejected with an error
    /// naming the path rather than silently dropped; rollback is complete.
    #[test]
    fn nested_submodule_under_published_parent_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let NestedFixture { sup, .. } = nested_fixture(tmp.path());

        let ws = superproject_workspace(&sup);
        let staging = tmp.path().join("staging");
        let TransferBundle {
            bundle_path,
            mut refs,
            ..
        } = create_transfer_bundle(&ws, &[], &staging).unwrap();
        refs.submodules.retain(|s| !s.published);
        let paths: Vec<&str> = refs.submodules.iter().map(|s| s.path.as_str()).collect();
        assert_eq!(paths, ["sub/inner"], "{refs:?}");

        let target = tempfile::tempdir().unwrap();
        let err = materialize_workspace_git_blocking(&bundle_path, &refs, &ws, &[], target.path())
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("hydrate submodule sub/inner failed") && msg.contains("not checked out"),
            "got {msg}"
        );
        assert!(!target.path().join(&ws.id.0).exists());
    }

    /// A superproject tree that commits a symlink pointing outside the
    /// workspace dir, with a manifest entry nested under it, is rejected
    /// before any git call runs through the link: the source repository the
    /// link resolves to is untouched and the rollback is complete.
    #[cfg(unix)]
    #[test]
    fn symlinked_parent_escaping_the_checkout_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let (sup, origin) = superproject_with_submodule(tmp.path());
        let sub = sup.join("sub");
        fgit(&sub, &["checkout", "-q", "main"]);
        local_commit(&sub, "wip.txt");
        // Committed symlink resolving to the source superproject itself: on
        // the target it points outside the materialized checkout, at a
        // repository whose tip records `sub` at the bundled commit.
        std::os::unix::fs::symlink(&sup, sup.join("evil")).unwrap();
        fgit(&sup, &["add", "evil"]);
        fgit(&sup, &["commit", "-q", "-m", "add symlink"]);

        let ws = superproject_workspace(&sup);
        let staging = tmp.path().join("staging");
        let TransferBundle {
            bundle_path,
            mut refs,
            ..
        } = create_transfer_bundle(&ws, &[], &staging).unwrap();
        assert_eq!(refs.submodules.len(), 1, "{refs:?}");
        refs.submodules[0].path = "evil/sub".to_string();
        let src_url_before = fgit(&sup, &["config", "submodule.sub.url"]);
        assert_eq!(src_url_before, origin.to_str().unwrap());

        let target = tempfile::tempdir().unwrap();
        let err = materialize_workspace_git_blocking(&bundle_path, &refs, &ws, &[], target.path())
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("hydrate submodule evil/sub failed")
                && msg.contains("outside the checkout"),
            "got {msg}"
        );
        assert!(
            !target.path().join(&ws.id.0).exists(),
            "rollback removed the workspace dir"
        );
        assert_eq!(
            fgit(&sup, &["config", "submodule.sub.url"]),
            src_url_before,
            "nothing was written through the symlink"
        );
        assert_no_config_mentions(&sup.join(".git"), staging.to_str().unwrap());
    }

    /// A manifest entry whose `name` is not the `.gitmodules` entry for its
    /// `path` at the containing tip is rejected before any clone: the bundle
    /// URL would otherwise be written under the wrong key and `submodule
    /// update` would resolve the path to its `.gitmodules` URL instead.
    #[test]
    fn submodule_name_must_match_gitmodules_entry_for_path() {
        let tmp = tempfile::tempdir().unwrap();
        let (sup, _origin) = superproject_with_submodule(tmp.path());
        let sub = sup.join("sub");
        fgit(&sub, &["checkout", "-q", "main"]);
        local_commit(&sub, "wip.txt");

        let ws = superproject_workspace(&sup);
        let staging = tmp.path().join("staging");
        let TransferBundle {
            bundle_path,
            mut refs,
            ..
        } = create_transfer_bundle(&ws, &[], &staging).unwrap();
        assert_eq!(refs.submodules.len(), 1);
        refs.submodules[0].name = "other".to_string();

        let target = tempfile::tempdir().unwrap();
        let err = materialize_workspace_git_blocking(&bundle_path, &refs, &ws, &[], target.path())
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("hydrate submodule sub failed")
                && msg.contains("submodule.other.path missing from .gitmodules"),
            "got {msg}"
        );
        assert!(!target.path().join(&ws.id.0).exists());
    }

    /// (d) An archive from an older daemon (no `submodules` key in
    /// `refs.json`, no submodule bundles) materializes exactly as before:
    /// the submodule stays an uninitialized gitlink directory.
    #[test]
    fn legacy_manifest_without_submodules_materializes_unchanged() {
        let tmp = tempfile::tempdir().unwrap();
        let (sup, _origin) = superproject_with_submodule(tmp.path());
        let sub = sup.join("sub");
        fgit(&sub, &["checkout", "-q", "main"]);
        local_commit(&sub, "wip.txt");

        let ws = superproject_workspace(&sup);
        let staging = tmp.path().join("staging");
        let TransferBundle {
            bundle_path, refs, ..
        } = create_transfer_bundle(&ws, &[], &staging).unwrap();
        assert_eq!(refs.submodules.len(), 1);
        fs::remove_dir_all(staging.join("submodules")).unwrap();

        let mut legacy = serde_json::to_value(&refs).unwrap();
        assert!(legacy
            .as_object_mut()
            .unwrap()
            .remove("submodules")
            .is_some());
        let legacy: TransferRefsManifest = serde_json::from_value(legacy).unwrap();
        assert!(legacy.submodules.is_empty());

        let target = tempfile::tempdir().unwrap();
        let out =
            materialize_workspace_git_blocking(&bundle_path, &legacy, &ws, &[], target.path())
                .unwrap();
        let dst_sub = out.checkout_dir.join("sub");
        assert!(dst_sub.is_dir());
        assert!(!dst_sub.join(".git").exists(), "gitlink left uninitialized");
        assert!(fgit(&out.checkout_dir, &["submodule", "status"]).starts_with('-'));
    }

    /// The async entry materializes the checkout without registering it in
    /// `known_repo` — the checkout is workspace-owned storage under the
    /// workspaces root and stays out of the registry
    /// (intent-hq/monorepo#2227).
    #[tokio::test]
    async fn async_entry_does_not_register_known_repo() {
        let tmp = crate::tests::TempDb::new();
        let store = intent_store::Store::open(&tmp.path)
            .await
            .expect("open store");

        let src = tempfile::TempDir::new().unwrap();
        let repo = src.path().join("source-repo");
        init_repo(&repo);
        let ws = workspace_for_repo(&repo);
        let TransferBundle {
            bundle_path, refs, ..
        } = create_transfer_bundle(&ws, &[], &src.path().join("staging")).unwrap();

        let target = tempfile::TempDir::new().unwrap();
        let out = materialize_workspace_git(
            bundle_path,
            refs,
            ws.clone(),
            vec![],
            target.path().to_path_buf(),
        )
        .await
        .unwrap();

        assert!(out.checkout_dir.join(".git").exists(), "checkout created");
        assert_eq!(
            store.list_known_repos().await.unwrap(),
            vec![],
            "materialized checkout is not registered"
        );
    }

    // -- remote state (intent-hq/intent#4438) -------------------------------

    /// Configure a remote and remote-tracking refs at the given shas — the
    /// published state a fetch would have produced, without any network.
    fn add_remote_with_tracking(repo_path: &Path, name: &str, url: &str, refs: &[(&str, &str)]) {
        let repo = git2::Repository::open(repo_path).unwrap();
        repo.remote(name, url).unwrap();
        for (branch, sha) in refs {
            repo.reference(
                &format!("refs/remotes/{name}/{branch}"),
                git2::Oid::from_str(sha).unwrap(),
                true,
                "test tracking ref",
            )
            .unwrap();
        }
    }

    fn ref_sha(repo_path: &Path, name: &str) -> Option<String> {
        let repo = git2::Repository::open(repo_path).unwrap();
        repo.find_reference(name)
            .ok()
            .and_then(|r| r.target())
            .map(|o| o.to_string())
    }

    fn config_value(repo_path: &Path, key: &str) -> Option<String> {
        let repo = git2::Repository::open(repo_path).unwrap();
        repo.config().unwrap().get_string(key).ok()
    }

    /// A dirty workspace on a published branch: origin (portable https URL),
    /// `origin/main` at the base, `origin/feature` at the branch tip, and the
    /// branch tracking `origin/feature`. After an offline import the checkout
    /// is still connected to origin with the same tracking refs and upstream,
    /// so the archive-facing count reports zero unpushed commits while the
    /// dirty state is still reported.
    #[test]
    fn roundtrip_restores_remote_tracking_refs_and_upstream() {
        let src = tempfile::TempDir::new().unwrap();
        let repo = src.path().join("source-repo");
        init_repo(&repo);
        let base = repo_head(&repo);
        fgit(&repo, &["checkout", "-q", "-b", "feature"]);
        let feature_tip = commit_file(&repo, "feature.txt", "feature\n", "feat: branch work");
        let url = "https://example.com/org/repo.git";
        add_remote_with_tracking(
            &repo,
            "origin",
            url,
            &[("main", &base), ("feature", &feature_tip)],
        );
        fgit(&repo, &["branch", "-q", "-u", "origin/feature", "feature"]);
        fs::write(repo.join("untracked.txt"), "untracked\n").unwrap();
        fs::write(repo.join("README.md"), "modified\n").unwrap();
        let src_changes = intent_git::local_changes(&repo).unwrap();
        assert_eq!(src_changes.unpushed_count, 0);
        assert_eq!(src_changes.uncommitted_count, 2);

        let mut ws = workspace_for_repo(&repo);
        ws.branch = "feature".to_string();
        ws.base_ref = Some("main".to_string());
        let staging = src.path().join("staging");
        let TransferBundle {
            bundle_path, refs, ..
        } = create_transfer_bundle(&ws, &[], &staging).unwrap();
        assert!(refs.workspace_wip_commit_sha.is_some());

        let target = tempfile::TempDir::new().unwrap();
        let out = materialize_workspace_git_blocking(&bundle_path, &refs, &ws, &[], target.path())
            .unwrap();
        let dst = &out.checkout_dir;

        assert_eq!(remote_names(dst), vec!["origin".to_string()]);
        assert_eq!(fgit(dst, &["remote", "get-url", "origin"]), url);
        assert_eq!(
            ref_sha(dst, "refs/remotes/origin/main").as_deref(),
            Some(base.as_str())
        );
        assert_eq!(
            ref_sha(dst, "refs/remotes/origin/feature").as_deref(),
            Some(feature_tip.as_str())
        );
        assert_eq!(
            config_value(dst, "branch.feature.remote").as_deref(),
            Some("origin")
        );
        assert_eq!(
            config_value(dst, "branch.feature.merge").as_deref(),
            Some("refs/heads/feature")
        );
        assert_eq!(repo_head(dst), feature_tip, "WIP unwound");
        let dst_changes = intent_git::local_changes(dst).unwrap();
        assert_eq!(dst_changes, src_changes, "archive-facing counts match");
        assert_no_config_mentions(&dst.join(".git"), staging.to_str().unwrap());
    }

    /// Bundle + materialize a remote-less clean repo on `feature` (one commit
    /// past `main`) after `prepare` has shaped its remote state; returns the
    /// materialized checkout.
    fn materialize_feature_repo(
        tmp: &Path,
        prepare: impl FnOnce(&Path, &str, &str),
    ) -> (PathBuf, TransferRefsManifest) {
        let repo = tmp.join("source-repo");
        init_repo(&repo);
        let base = repo_head(&repo);
        fgit(&repo, &["checkout", "-q", "-b", "feature"]);
        let tip = commit_file(&repo, "feature.txt", "feature\n", "feat: branch work");
        prepare(&repo, &base, &tip);
        let mut ws = workspace_for_repo(&repo);
        ws.branch = "feature".to_string();
        ws.base_ref = Some("main".to_string());
        let TransferBundle {
            bundle_path, refs, ..
        } = create_transfer_bundle(&ws, &[], &tmp.join("staging")).unwrap();
        let target = tmp.join("target");
        let out =
            materialize_workspace_git_blocking(&bundle_path, &refs, &ws, &[], &target).unwrap();
        (out.checkout_dir, refs)
    }

    /// `origin/feature` two commits behind the local branch: the import
    /// reports exactly those two as unpushed, not the whole history.
    #[test]
    fn ahead_branch_reports_exact_unpushed_count_after_import() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (dst, _) = materialize_feature_repo(tmp.path(), |repo, base, tip| {
            add_remote_with_tracking(
                repo,
                "origin",
                "https://example.com/org/repo.git",
                &[("main", base), ("feature", tip)],
            );
            commit_file(repo, "a.txt", "a\n", "feat: local a");
            commit_file(repo, "b.txt", "b\n", "feat: local b");
            assert_eq!(intent_git::local_changes(repo).unwrap().unpushed_count, 2);
        });
        let changes = intent_git::local_changes(&dst).unwrap();
        assert!(changes.has_remote_refs);
        assert_eq!(changes.unpushed_count, 2);
    }

    /// A branch never pushed, off a published base: only the branch's own
    /// commit counts, because `origin/main` still bounds the walk.
    #[test]
    fn never_pushed_branch_with_published_base_counts_only_new_commits() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (dst, refs) = materialize_feature_repo(tmp.path(), |repo, base, _tip| {
            add_remote_with_tracking(
                repo,
                "origin",
                "https://example.com/org/repo.git",
                &[("main", base)],
            );
        });
        assert_eq!(refs.workspace_upstream, None, "no upstream to restore");
        assert_eq!(config_value(&dst, "branch.feature.remote"), None);
        let changes = intent_git::local_changes(&dst).unwrap();
        assert!(changes.has_remote_refs);
        assert_eq!(changes.unpushed_count, 1);
    }

    /// No remotes on the source: the import stays remote-less and reports
    /// the whole history as unpushed, exactly as before.
    #[test]
    fn remote_less_source_keeps_truthful_count() {
        let tmp = tempfile::TempDir::new().unwrap();
        let (dst, refs) = materialize_feature_repo(tmp.path(), |_, _, _| {});
        assert!(refs.remotes.is_empty());
        assert!(remote_names(&dst).is_empty());
        let changes = intent_git::local_changes(&dst).unwrap();
        assert!(!changes.has_remote_refs);
        assert_eq!(changes.unpushed_count, 2);
    }

    /// Two non-origin remotes (scp-like and ssh URLs, one with a push URL),
    /// tracking tips ahead of and diverged from HEAD, and the branch tracking
    /// the fork: everything is restored as configured on the source, the
    /// sandbox stays remote-less, and no `origin` is invented.
    #[test]
    fn multiple_non_origin_remotes_roundtrip() {
        let src = tempfile::TempDir::new().unwrap();
        let repo = src.path().join("source-repo");
        init_repo(&repo);
        let base = repo_head(&repo);
        fgit(&repo, &["checkout", "-q", "-b", "feature"]);
        let tip = commit_file(&repo, "feature.txt", "feature\n", "feat: branch work");
        // upstream/main is AHEAD of the base (a commit the branch lacks);
        // fork/feature has DIVERGED (a commit off the base, not on the branch).
        fgit(&repo, &["checkout", "-q", "main"]);
        let upstream_tip = commit_file(&repo, "up.txt", "up\n", "feat: upstream ahead");
        fgit(&repo, &["checkout", "-q", "-b", "diverged", &base]);
        let fork_tip = commit_file(&repo, "fork.txt", "fork\n", "feat: fork diverged");
        fgit(&repo, &["checkout", "-q", "feature"]);
        fgit(&repo, &["branch", "-q", "-D", "diverged"]);
        add_remote_with_tracking(
            &repo,
            "upstream",
            "git@example.com:org/repo.git",
            &[("main", &upstream_tip)],
        );
        add_remote_with_tracking(
            &repo,
            "fork",
            "ssh://git@example.com/me/repo.git",
            &[("feature", &fork_tip), ("main", &base)],
        );
        fgit(
            &repo,
            &[
                "remote",
                "set-url",
                "--push",
                "fork",
                "https://example.com/me/repo.git",
            ],
        );
        fgit(&repo, &["branch", "-q", "-u", "fork/feature", "feature"]);
        let src_changes = intent_git::local_changes(&repo).unwrap();
        assert_eq!(src_changes.unpushed_count, 1, "only the branch commit");

        let mut ws = workspace_for_repo(&repo);
        ws.branch = "feature".to_string();
        ws.base_ref = Some("main".to_string());
        let agent = AgentId::new();
        let branch = format!("sb/{}", agent.0);
        let sb_src = src.path().join("sandbox");
        make_sandbox_clone(&repo, &sb_src, &branch);
        fgit(&sb_src, &["remote", "remove", "origin"]);
        let sb = sandbox_row(&ws, &agent, &sb_src, &branch);
        let staging = src.path().join("staging");
        let TransferBundle {
            bundle_path, refs, ..
        } = create_transfer_bundle(&ws, std::slice::from_ref(&sb), &staging).unwrap();
        let names: Vec<&str> = refs.remotes.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, ["fork", "upstream"]);
        assert_eq!(
            refs.workspace_upstream,
            Some(BranchUpstream {
                remote: "fork".to_string(),
                merge_ref: "refs/heads/feature".to_string(),
            })
        );

        let target = tempfile::TempDir::new().unwrap();
        let out = materialize_workspace_git_blocking(
            &bundle_path,
            &refs,
            &ws,
            std::slice::from_ref(&sb),
            target.path(),
        )
        .unwrap();
        let dst = &out.checkout_dir;
        assert_eq!(
            remote_names(dst),
            vec!["fork".to_string(), "upstream".to_string()]
        );
        assert_eq!(
            fgit(dst, &["remote", "get-url", "upstream"]),
            "git@example.com:org/repo.git"
        );
        assert_eq!(
            fgit(dst, &["remote", "get-url", "fork"]),
            "ssh://git@example.com/me/repo.git"
        );
        assert_eq!(
            fgit(dst, &["remote", "get-url", "--push", "fork"]),
            "https://example.com/me/repo.git"
        );
        assert_eq!(
            ref_sha(dst, "refs/remotes/upstream/main").as_deref(),
            Some(upstream_tip.as_str())
        );
        assert_eq!(
            ref_sha(dst, "refs/remotes/fork/feature").as_deref(),
            Some(fork_tip.as_str())
        );
        assert_eq!(
            ref_sha(dst, "refs/remotes/fork/main").as_deref(),
            Some(base.as_str())
        );
        assert_eq!(repo_head(dst), tip);
        assert_eq!(
            fgit(dst, &["rev-parse", "--abbrev-ref", "feature@{upstream}"]),
            "fork/feature"
        );
        assert_eq!(intent_git::local_changes(dst).unwrap(), src_changes);
        assert_eq!(out.sandboxes.len(), 1);
        assert!(
            remote_names(&out.sandboxes[0].path).is_empty(),
            "sandboxes stay remote-less"
        );
        assert_no_config_mentions(&dst.join(".git"), staging.to_str().unwrap());
    }

    /// Unsafe or machine-local remotes never enter the manifest: a local
    /// path, a `file://` URL and a remote-helper address are skipped (their
    /// tracking refs with them), and an upstream naming a skipped remote is
    /// not recorded. Credential-bearing URLs survive stripped — fetch and
    /// push URLs alike — so the remote stays connected while the archive's
    /// manifest and the target's git config never contain the secret.
    #[test]
    fn unsafe_remotes_are_skipped_without_leaking_values() {
        let tmp = tempfile::TempDir::new().unwrap();
        let local_origin = tmp.path().join("local-origin.git");
        let (dst, refs) = materialize_feature_repo(tmp.path(), |repo, base, tip| {
            add_remote_with_tracking(
                repo,
                "local",
                local_origin.to_str().unwrap(),
                &[("main", base), ("feature", tip)],
            );
            add_remote_with_tracking(repo, "filed", "file:///srv/git/repo.git", &[("main", base)]);
            add_remote_with_tracking(repo, "helper", "ext::sh -c touch% /tmp/pwned", &[]);
            add_remote_with_tracking(
                repo,
                "tokenish",
                "https://ghp_abcdef:s3cret-token@example.com/org/repo.git",
                &[("main", base)],
            );
            fgit(
                repo,
                &[
                    "remote",
                    "set-url",
                    "--push",
                    "tokenish",
                    "ssh://git:s3cret-push@example.com/org/repo.git",
                ],
            );
            fgit(repo, &["branch", "-q", "-u", "local/feature", "feature"]);
        });
        let names: Vec<&str> = refs.remotes.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, ["tokenish"], "{refs:?}");
        assert_eq!(refs.remotes[0].url, "https://example.com/org/repo.git");
        assert_eq!(
            refs.remotes[0].push_url.as_deref(),
            Some("ssh://git@example.com/org/repo.git")
        );
        assert_eq!(refs.workspace_upstream, None);
        let manifest = serde_json::to_string(&refs).unwrap();
        assert!(!manifest.contains("s3cret"), "{manifest}");
        assert!(!manifest.contains("ghp_"), "{manifest}");
        assert!(!manifest.contains("pwned"), "{manifest}");
        assert!(!manifest.contains(local_origin.to_str().unwrap()));

        assert_eq!(remote_names(&dst), vec!["tokenish".to_string()]);
        assert_eq!(
            fgit(&dst, &["remote", "get-url", "tokenish"]),
            "https://example.com/org/repo.git"
        );
        assert_eq!(
            fgit(&dst, &["remote", "get-url", "--push", "tokenish"]),
            "ssh://git@example.com/org/repo.git"
        );
        assert_eq!(ref_sha(&dst, "refs/remotes/local/feature"), None);
        assert_eq!(ref_sha(&dst, "refs/remotes/local/main"), None);
        assert_eq!(ref_sha(&dst, "refs/remotes/filed/main"), None);
        assert_eq!(config_value(&dst, "branch.feature.remote"), None);
        assert_no_config_mentions(&dst.join(".git"), "s3cret");
        assert_no_config_mentions(&dst.join(".git"), "ghp_");
        assert_no_config_mentions(&dst.join(".git"), "pwned");
        // The published prefix under the surviving remote keeps the count
        // truthful: only the branch commit is unpushed.
        assert_eq!(intent_git::local_changes(&dst).unwrap().unpushed_count, 1);
    }

    type Tamper = Box<dyn Fn(&mut TransferRefsManifest)>;

    /// Manifest metadata is re-validated on import: a tracking ref outside
    /// the remote's namespace, a tracking ref at the WIP snapshot commit, an
    /// unsafe URL, an unsafe remote name, and an upstream naming an unknown
    /// remote each fail the import and roll the target back.
    #[test]
    fn invalid_remote_metadata_fails_import_and_rolls_back() {
        let src = tempfile::TempDir::new().unwrap();
        let repo = src.path().join("source-repo");
        init_repo(&repo);
        let base = repo_head(&repo);
        add_remote_with_tracking(
            &repo,
            "origin",
            "https://example.com/org/repo.git",
            &[("main", &base)],
        );
        fs::write(repo.join("dirty.txt"), "dirty\n").unwrap();
        let ws = workspace_for_repo(&repo);
        let TransferBundle {
            bundle_path, refs, ..
        } = create_transfer_bundle(&ws, &[], &src.path().join("staging")).unwrap();
        let wip = refs.workspace_wip_commit_sha.clone().unwrap();

        let tamper: Vec<(&str, Tamper)> = vec![
            (
                "ref outside namespace",
                Box::new(|m| {
                    m.remotes[0].tracking_refs[0].ref_name = "refs/heads/main".to_string();
                }),
            ),
            (
                "ref escaping namespace",
                Box::new(|m| {
                    m.remotes[0].tracking_refs[0].ref_name =
                        "refs/remotes/origin/../../heads/main".to_string();
                }),
            ),
            (
                "tracking ref at WIP commit",
                Box::new(move |m| m.remotes[0].tracking_refs[0].sha.clone_from(&wip)),
            ),
            (
                "unsafe url",
                Box::new(|m| m.remotes[0].url = "ext::sh -c id".to_string()),
            ),
            (
                "local path url",
                Box::new(|m| m.remotes[0].url = "/srv/git/repo.git".to_string()),
            ),
            (
                "unsafe name",
                Box::new(|m| m.remotes[0].name = "--upload-pack=id".to_string()),
            ),
            (
                "unknown upstream remote",
                Box::new(|m| {
                    m.workspace_upstream = Some(BranchUpstream {
                        remote: "ghost".to_string(),
                        merge_ref: "refs/heads/main".to_string(),
                    });
                }),
            ),
            (
                "upstream merge ref outside heads",
                Box::new(|m| {
                    m.workspace_upstream = Some(BranchUpstream {
                        remote: "origin".to_string(),
                        merge_ref: "refs/tags/v1".to_string(),
                    });
                }),
            ),
        ];
        for (case, mutate) in tamper {
            let mut bad = refs.clone();
            mutate(&mut bad);
            let target = tempfile::TempDir::new().unwrap();
            let err =
                materialize_workspace_git_blocking(&bundle_path, &bad, &ws, &[], target.path());
            assert!(err.is_err(), "{case}: import must fail");
            assert!(
                !target.path().join(&ws.id.0).exists(),
                "{case}: workspace dir rolled back"
            );
        }

        // A credential-bearing URL in the manifest is stripped, not stored.
        let mut cred = refs.clone();
        cred.remotes[0].url = "ssh://u:s3cret@example.com/r.git".to_string();
        let target = tempfile::TempDir::new().unwrap();
        let out = materialize_workspace_git_blocking(&bundle_path, &cred, &ws, &[], target.path())
            .unwrap();
        assert_eq!(
            fgit(&out.checkout_dir, &["remote", "get-url", "origin"]),
            "ssh://u@example.com/r.git"
        );
        assert_no_config_mentions(&out.checkout_dir.join(".git"), "s3cret");

        // The untampered manifest still imports.
        let target = tempfile::TempDir::new().unwrap();
        materialize_workspace_git_blocking(&bundle_path, &refs, &ws, &[], target.path()).unwrap();
    }

    /// An archive from a daemon that predates remote capture (no `remotes`
    /// / `workspaceUpstream` keys) imports exactly as before: no remote is
    /// guessed.
    #[test]
    fn legacy_manifest_without_remote_fields_imports_remote_less() {
        let src = tempfile::TempDir::new().unwrap();
        let repo = src.path().join("source-repo");
        init_repo(&repo);
        add_remote_with_tracking(
            &repo,
            "origin",
            "https://example.com/org/repo.git",
            &[("main", &repo_head(&repo))],
        );
        let ws = workspace_for_repo(&repo);
        let TransferBundle {
            bundle_path, refs, ..
        } = create_transfer_bundle(&ws, &[], &src.path().join("staging")).unwrap();
        let mut json: serde_json::Value = serde_json::to_value(&refs).unwrap();
        let obj = json.as_object_mut().unwrap();
        assert!(obj.remove("remotes").is_some());
        obj.remove("workspaceUpstream");
        let legacy: TransferRefsManifest = serde_json::from_value(json).unwrap();
        assert!(legacy.remotes.is_empty());
        assert_eq!(legacy.workspace_upstream, None);

        let target = tempfile::TempDir::new().unwrap();
        let out =
            materialize_workspace_git_blocking(&bundle_path, &legacy, &ws, &[], target.path())
                .unwrap();
        assert!(remote_names(&out.checkout_dir).is_empty());
        assert_eq!(ref_sha(&out.checkout_dir, "refs/remotes/origin/main"), None);
    }
}
