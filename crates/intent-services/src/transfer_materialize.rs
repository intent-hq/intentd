//! Target-side git materialization for workspace transfer (spec §4, resolved
//! decision 1): at import-commit time, recreate the workspace checkout and
//! every transferred sandbox from the received bundle. The checkout is cloned
//! from the bundle at the workspace branch, the base ref is fetched as a
//! local branch, sandboxes are re-provisioned as CoW clones of the checkout
//! (plain clone when CoW is unavailable) with their `sb/<agentId>` branches
//! fetched from the bundle, and the sentinel WIP snapshot commits are unwound
//! so staged/unstaged/untracked state lands exactly as it was on the source.
//! All-or-nothing: any failure removes everything this module created. No
//! wire code lives here.

use std::path::{Path, PathBuf};

use intent_core::{CheckoutMode, Error, Result, Workspace};
use intent_git::{cow_clone, cow_probe, CowSupport};
use intent_store::Sandbox;

use crate::transfer_git::{run_git, unwind_wip, TransferRefsManifest};

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
    /// standalone (`CheckoutMode::Direct`) with no remotes configured — the
    /// bundle was the only source and its staging path must not leak into
    /// the repo config.
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
/// bundle path must not persist), fetch the base ref as a local branch,
/// re-provision each sandbox as a CoW clone of the checkout (plain local
/// clone when CoW is unavailable) with its branch fetched from the bundle
/// and checked out, then unwind the WIP snapshot commits (sandboxes first,
/// then the workspace) so the dirty state lands exactly as captured.
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

    // 4. Unwind the workspace WIP snapshot last, restoring the exact
    //    staged/unstaged/untracked split on the workspace branch.
    if refs.workspace_wip_commit_sha.is_some() && !unwind_wip(&checkout_dir)? {
        return Err(Error::Internal(
            "manifest records a workspace WIP snapshot but the checkout tip is not one".to_string(),
        ));
    }

    Ok(MaterializedGit {
        checkout_dir,
        workspace_branch: refs.workspace_branch.clone(),
        base_sha: refs.base_sha.clone(),
        sandboxes: materialized,
        skipped_agent_ids: skipped,
    })
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
    use crate::transfer_git::create_transfer_bundle;
    use intent_core::{AgentId, WorkspaceId, WorkspaceStatus};
    use intent_store::SandboxStatus;
    use std::fs;
    use std::process::Command;

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

    /// Status entries as (path, staged, wt_modified, untracked) tuples,
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
        let (bundle_path, refs) =
            create_transfer_bundle(&ws, std::slice::from_ref(&sb), &staging).unwrap();

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

        let (bundle_path, refs) =
            create_transfer_bundle(&ws, &[], &src.path().join("staging")).unwrap();
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
        let (bundle_path, refs) =
            create_transfer_bundle(&ws, &[], &src.path().join("staging")).unwrap();

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

        let (bundle_path, mut refs) =
            create_transfer_bundle(&ws, &[], &src.path().join("staging")).unwrap();
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
        let (bundle_path, refs) =
            create_transfer_bundle(&ws, &[], &src.path().join("staging")).unwrap();
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
        let (bundle_path, refs) =
            create_transfer_bundle(&ws, &[], &src.path().join("staging")).unwrap();

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
        let (bundle_path, mut refs) =
            create_transfer_bundle(&ws, std::slice::from_ref(&sb), &staging).unwrap();

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
        let (bundle_path, refs) =
            create_transfer_bundle(&ws, &[], &src.path().join("staging")).unwrap();

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
}
