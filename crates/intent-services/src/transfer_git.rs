//! Source-side git packaging for workspace transfer (spec §4, resolved
//! decision 1): snapshot dirty state as sentinel-marked WIP commits (the
//! workspace worktree AND every sandbox), then build one `git bundle`
//! carrying the workspace branch, the base ref, and all `sb/<agentId>`
//! sandbox branches — plus one self-contained bundle per tracked submodule
//! whose checked-out commit is unpublished (monorepo#4219), so the target
//! can hydrate it without a network. The inverse helper ([`unwind_wip`]) removes a WIP
//! snapshot commit and restores the exact staged/unstaged/untracked split —
//! reused by the import side after materialization and by the source-side
//! failure/cleanup paths. No wire code lives here.

use std::path::{Path, PathBuf};
use std::process::Command;

use intent_core::{Error, Result, Workspace};
use intent_store::Sandbox;
use serde::{Deserialize, Serialize};

use crate::nested_repos::{is_dirty, stage_all_skipping_nested};
use crate::transfer_submodules::{find_unpublished_submodules, UnpublishedSubmodule};

/// First line of every transfer WIP snapshot commit message. The import side
/// identifies snapshot commits by this sentinel and unwinds them via
/// [`unwind_wip`]; keep it stable across versions.
pub(crate) const TRANSFER_WIP_SENTINEL: &str = "intent-transfer: WIP snapshot";

/// Commit-message trailer carrying the pre-snapshot index tree OID, so
/// [`unwind_wip`] can restore the exact staged/unstaged split (a plain soft
/// reset would leave everything staged).
const INDEX_TREE_TRAILER: &str = "Intent-Index-Tree:";

/// Namespace for the temporary refs anchoring non-branch bundle entries in
/// the worktree repo. Created for the duration of `git bundle create` and
/// deleted afterwards (success or failure); the names survive inside the
/// bundle header, which is how the target addresses them.
const TRANSFER_REF_NS: &str = "refs/intent/transfer";

/// Bundle ref name recording the workspace base commit. Namespaced by
/// workspace id: worktree-based workspaces share the repository's common
/// refs dir, so concurrent transfers must not collide on temp-ref names.
pub(crate) fn base_bundle_ref(workspace_id: &str) -> String {
    format!("{TRANSFER_REF_NS}/{workspace_id}/base")
}

/// Bundle ref name recording one sandbox's `sb/<agentId>` branch tip
/// (workspace-namespaced like [`base_bundle_ref`]).
pub(crate) fn sandbox_bundle_ref(workspace_id: &str, agent_id: &str) -> String {
    format!("{TRANSFER_REF_NS}/{workspace_id}/sandbox/{agent_id}")
}

/// Temp ref anchoring the `index`-th unpublished submodule commit inside ITS
/// OWN repository for the duration of its `git bundle create` (the name
/// survives in that bundle's header, workspace-namespaced like the others).
pub(crate) fn submodule_bundle_ref(workspace_id: &str, index: usize) -> String {
    format!("{TRANSFER_REF_NS}/{workspace_id}/submodule/{index}")
}

/// Archive entry name of the `index`-th submodule bundle
/// (`git/submodules/<n>.bundle`). Index-based so submodule paths never need
/// encoding into zip entry names.
pub(crate) fn submodule_bundle_entry(index: usize) -> String {
    format!("git/submodules/{index}.bundle")
}

/// Output of [`create_transfer_bundle`]: the loose bundle files in the
/// staging dir plus the ref inventory that becomes `git/refs.json`.
#[derive(Debug)]
pub struct TransferBundle {
    /// The superproject bundle (`<staging>/<wsId>.bundle`), written to the
    /// archive as `git/repo.bundle`.
    pub bundle_path: PathBuf,
    /// One `(loose file, archive entry)` pair per bundled unpublished
    /// submodule, in `refs.submodules` order; the entry is
    /// [`SubmoduleBundleRef::bundle_entry`].
    pub submodule_bundles: Vec<(PathBuf, String)>,
    pub refs: TransferRefsManifest,
}

/// Ref inventory of a transfer bundle: what each bundle ref is and how it
/// maps back to workspace/sandbox branches on the target.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferRefsManifest {
    /// Branch name the workspace worktree was on (normally `Workspace.branch`).
    pub workspace_branch: String,
    /// Bundle ref carrying the workspace branch (`refs/heads/<branch>`).
    pub workspace_bundle_ref: String,
    /// Tip of the workspace branch as bundled (the WIP snapshot commit when
    /// the worktree was dirty).
    pub workspace_head_sha: String,
    /// WIP snapshot commit created on the workspace branch, if the worktree
    /// was dirty. Left in place on success — the orchestrator unwinds it via
    /// [`unwind_wip`] once the export settles.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_wip_commit_sha: Option<String>,
    /// The workspace `baseRef` name (e.g. `main`), when set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_ref: Option<String>,
    /// Bundle ref anchoring the base commit ([`base_bundle_ref`]); omitted
    /// when no base could be resolved locally.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_bundle_ref: Option<String>,
    /// The resolved base commit SHA.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_sha: Option<String>,
    /// One entry per sandbox whose branch made it into the bundle.
    pub sandboxes: Vec<SandboxBundleRef>,
    /// One entry per tracked worktree submodule whose checked-out commit is
    /// unpublished (unreachable from any of its remote-tracking refs) and
    /// therefore rides the archive as its own bundle. Ordered by path, so a
    /// parent submodule always precedes its nested children. Absent/empty in
    /// archives from older daemons and when every submodule is published.
    #[serde(default)]
    pub submodules: Vec<SubmoduleBundleRef>,
}

/// One unpublished submodule as bundled into the archive.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmoduleBundleRef {
    /// The `submodule.<name>` key in ITS superproject (raw, not composed).
    pub name: String,
    /// Path relative to the workspace worktree root, forward slashes; nested
    /// submodules compose their parents' paths (`sub/inner`), so the
    /// containing repository is the entry whose path is this path's parent
    /// (or the worktree itself).
    pub path: String,
    /// The bundled commit — the submodule checkout's HEAD, which is the
    /// gitlink recorded by the (possibly WIP-snapshotted) superproject tip.
    pub commit_sha: String,
    /// Branch the submodule checkout had HEAD on, when attached.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    /// The checkout's `remote.origin.url`, when configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin_url: Option<String>,
    /// Archive entry carrying the bundle (`git/submodules/<n>.bundle`).
    pub bundle_entry: String,
    /// Ref inside that bundle anchoring `commit_sha`
    /// ([`submodule_bundle_ref`]); the bundle also carries `HEAD` at the same
    /// commit so a plain clone from it checks out.
    pub bundle_ref: String,
}

/// One sandbox branch as recorded in the bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxBundleRef {
    pub agent_id: String,
    /// The sandbox branch name (`sb/<agentId>`).
    pub branch: String,
    /// Bundle ref carrying the branch (`refs/intent/transfer/sandbox/<agentId>`).
    pub bundle_ref: String,
    /// Tip of the sandbox branch as bundled (the WIP snapshot commit when the
    /// sandbox was dirty).
    pub head_sha: String,
    /// WIP snapshot commit created in the sandbox, if it was dirty. Left in
    /// place on success, like the workspace one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wip_commit_sha: Option<String>,
    /// The sandbox's recorded provisioning base (`Sandbox.base_commit_sha`).
    pub base_commit_sha: String,
    /// The provisioning-time dirty-state snapshot (`Sandbox.snapshot_commit_sha`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_commit_sha: Option<String>,
}

/// Snapshot a repository's dirty state (staged + unstaged + untracked) as a
/// single sentinel-marked WIP commit on HEAD's branch. Returns the commit SHA,
/// or `None` when the repository is clean. The pre-snapshot index tree is
/// recorded as a commit-message trailer so [`unwind_wip`] restores the exact
/// staged/unstaged split, and anchored via an auxiliary second parent so it
/// stays reachable inside a transfer bundle.
pub(crate) fn snapshot_wip(repo_path: &Path) -> Result<Option<String>> {
    let repo = git2::Repository::open(repo_path)
        .map_err(|e| Error::Internal(format!("open repo for WIP snapshot failed: {e}")))?;
    if !is_dirty(&repo)? {
        return Ok(None);
    }
    let head_commit = repo
        .head()
        .ok()
        .and_then(|h| h.peel_to_commit().ok())
        .ok_or_else(|| {
            Error::Internal("cannot snapshot dirty state: repository HEAD is unborn".to_string())
        })?;
    let mut index = repo
        .index()
        .map_err(|e| Error::Internal(format!("get index failed: {e}")))?;
    // Pre-snapshot staged state, before add_all coarsens it.
    let orig_index_tree = index
        .write_tree()
        .map_err(|e| Error::Internal(format!("write pre-snapshot index tree failed: {e}")))?;
    // Untracked nested repos/worktrees cannot be staged: libgit2's add_all
    // rejects their paths outright (`invalid path`), and `git add` skips
    // embedded repos too. [`stage_all_skipping_nested`] filters them out via
    // the matched-path callback — no gitlink entries, the directories stay
    // untouched on disk.
    let nested = stage_all_skipping_nested(&repo, &mut index)?;
    if !nested.is_empty() {
        tracing::warn!(
            path = %repo_path.display(),
            skipped = ?nested,
            "WIP snapshot: skipping untracked nested git repos/worktrees; they will not travel with the export"
        );
    }
    index
        .write()
        .map_err(|e| Error::Internal(format!("write index failed: {e}")))?;
    // From here the on-disk index has everything staged; if anything below
    // fails there is no WIP commit for `unwind_wip` to detect, so restore the
    // pre-snapshot index ourselves to keep the no-mutation-on-failure
    // guarantee.
    let commit_result = (|| -> Result<git2::Oid> {
        let tree_oid = index
            .write_tree()
            .map_err(|e| Error::Internal(format!("write tree failed: {e}")))?;
        let tree = repo
            .find_tree(tree_oid)
            .map_err(|e| Error::Internal(format!("find tree failed: {e}")))?;
        let sig = resolve_signature(&repo)?;
        // Auxiliary anchor commit (no ref update): its only job is to make
        // the pre-snapshot index tree REACHABLE from the WIP commit, so the
        // tree travels inside a transfer bundle and `unwind_wip` on the
        // import side can restore the exact staged/unstaged split. Without
        // it the trailer OID would dangle in a bundle clone (bundles carry
        // only objects reachable from their refs) and the unwind would
        // degrade to everything-staged.
        let index_tree = repo
            .find_tree(orig_index_tree)
            .map_err(|e| Error::Internal(format!("find pre-snapshot index tree failed: {e}")))?;
        let anchor_oid = repo
            .commit(
                None,
                &sig,
                &sig,
                "intent-transfer: index state anchor",
                &index_tree,
                &[&head_commit],
            )
            .map_err(|e| Error::Internal(format!("create index anchor commit failed: {e}")))?;
        let anchor = repo
            .find_commit(anchor_oid)
            .map_err(|e| Error::Internal(format!("find index anchor commit failed: {e}")))?;
        let message = format!("{TRANSFER_WIP_SENTINEL}\n\n{INDEX_TREE_TRAILER} {orig_index_tree}");
        repo.commit(
            Some("HEAD"),
            &sig,
            &sig,
            &message,
            &tree,
            &[&head_commit, &anchor],
        )
        .map_err(|e| Error::Internal(format!("create WIP snapshot commit failed: {e}")))
    })();
    match commit_result {
        Ok(oid) => Ok(Some(oid.to_string())),
        Err(e) => {
            if let Ok(tree) = repo.find_tree(orig_index_tree) {
                let restored = index.read_tree(&tree).and_then(|()| index.write());
                if let Err(re) = restored {
                    tracing::warn!(
                        path = %repo_path.display(),
                        error = %re,
                        "WIP snapshot failed and pre-snapshot index could not be restored"
                    );
                }
            }
            Err(e)
        }
    }
}

/// Inverse of [`snapshot_wip`]: when HEAD is a transfer WIP snapshot commit,
/// soft-reset it away and restore the pre-snapshot index from the
/// [`INDEX_TREE_TRAILER`], leaving the worktree exactly as found (staged
/// stays staged, unstaged stays unstaged, untracked stays untracked).
/// Returns `false` (no-op) when HEAD is not a transfer WIP commit.
pub(crate) fn unwind_wip(repo_path: &Path) -> Result<bool> {
    let repo = git2::Repository::open(repo_path)
        .map_err(|e| Error::Internal(format!("open repo for WIP unwind failed: {e}")))?;
    let Some(head_commit) = repo.head().ok().and_then(|h| h.peel_to_commit().ok()) else {
        return Ok(false);
    };
    let message = head_commit.message().unwrap_or("").to_string();
    if !message.starts_with(TRANSFER_WIP_SENTINEL) {
        return Ok(false);
    }
    let parent = head_commit
        .parent(0)
        .map_err(|e| Error::Internal(format!("transfer WIP snapshot commit has no parent: {e}")))?;
    repo.reset(parent.as_object(), git2::ResetType::Soft, None)
        .map_err(|e| Error::Internal(format!("soft reset of WIP snapshot failed: {e}")))?;
    // Restore the exact pre-snapshot staged state; without the trailer the
    // soft reset alone leaves everything staged (degraded but safe).
    if let Some(tree_oid) = parse_index_tree_trailer(&message) {
        if let Ok(tree) = repo.find_tree(tree_oid) {
            let mut index = repo
                .index()
                .map_err(|e| Error::Internal(format!("get index failed: {e}")))?;
            index
                .read_tree(&tree)
                .map_err(|e| Error::Internal(format!("restore pre-snapshot index failed: {e}")))?;
            index
                .write()
                .map_err(|e| Error::Internal(format!("write restored index failed: {e}")))?;
        }
    }
    Ok(true)
}

/// Build the transfer bundle for a workspace: WIP-snapshot the worktree and
/// every sandbox, anchor the base commit and each sandbox branch under
/// temporary `refs/intent/transfer/*` refs in the worktree repo, and run
/// `git bundle create` + `verify`. Every tracked worktree submodule whose
/// checked-out commit is unpublished additionally gets its own
/// self-contained bundle (`<staging>/submodules/<n>.bundle`). Returns the
/// bundle paths and the ref inventory.
///
/// On success the WIP snapshot commits are left in place (they are what the
/// bundle refs point at); the caller unwinds them via [`unwind_wip`] once the
/// export settles. On failure every created WIP commit is unwound, temporary
/// refs are deleted, and any partial bundle file (submodule bundles included)
/// is removed — the source is restored exactly as found.
///
/// This is blocking work (git2 I/O plus `git` child processes); async callers
/// must run it via `spawn_blocking`, like the plan op does for
/// `estimate_bundle_bytes`.
///
/// # Errors
///
/// Returns `Error::Internal` if the workspace has no worktree, the staging directory cannot be created, or building the bundle fails.
pub fn create_transfer_bundle(
    ws: &Workspace,
    sandboxes: &[Sandbox],
    staging_dir: &Path,
) -> Result<TransferBundle> {
    let worktree = crate::git_ops::worktree_path(ws).ok_or_else(|| {
        Error::Internal("workspace has no worktree or repository path".to_string())
    })?;
    std::fs::create_dir_all(staging_dir)
        .map_err(|e| Error::Internal(format!("create staging dir failed: {e}")))?;
    let bundle_path = staging_dir.join(format!("{}.bundle", ws.id.0));
    let submodules_dir = staging_dir.join("submodules");

    let mut snapshotted: Vec<PathBuf> = Vec::new();
    let mut temp_refs: Vec<String> = Vec::new();
    let mut submodule_bundles: Vec<(PathBuf, String)> = Vec::new();
    let result = build_bundle(
        ws,
        sandboxes,
        &worktree,
        &bundle_path,
        &submodules_dir,
        &mut snapshotted,
        &mut temp_refs,
        &mut submodule_bundles,
    );

    // The temp refs only anchor the bundle build; drop them regardless of
    // outcome (their names live on inside the bundle header).
    cleanup_temp_refs(&worktree, &temp_refs);

    if result.is_err() {
        for path in snapshotted.iter().rev() {
            if let Err(e) = unwind_wip(path) {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "transfer bundle cleanup: failed to unwind WIP snapshot"
                );
            }
        }
        if bundle_path.exists() {
            let _ = std::fs::remove_file(&bundle_path);
        }
        for (path, _) in &submodule_bundles {
            if path.exists() {
                let _ = std::fs::remove_file(path);
            }
        }
        if submodules_dir.is_dir() {
            let _ = std::fs::remove_dir(&submodules_dir);
        }
    }

    result.map(|refs| TransferBundle {
        bundle_path,
        submodule_bundles,
        refs,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_bundle(
    ws: &Workspace,
    sandboxes: &[Sandbox],
    worktree: &Path,
    bundle_path: &Path,
    submodules_dir: &Path,
    snapshotted: &mut Vec<PathBuf>,
    temp_refs: &mut Vec<String>,
    submodule_bundles: &mut Vec<(PathBuf, String)>,
) -> Result<TransferRefsManifest> {
    let repo = git2::Repository::open(worktree)
        .map_err(|e| Error::Internal(format!("open workspace repo failed: {e}")))?;

    // 1. Snapshot the worktree's dirty state onto its branch.
    let workspace_wip = snapshot_wip(worktree)?;
    if workspace_wip.is_some() {
        snapshotted.push(worktree.to_path_buf());
    }

    // 2. The workspace branch is whatever HEAD points at — that is where the
    //    WIP snapshot landed. A detached HEAD has no branch to bundle.
    let head = repo
        .head()
        .map_err(|e| Error::Internal(format!("resolve workspace HEAD failed: {e}")))?;
    if !head.is_branch() {
        return Err(Error::Internal(
            "workspace HEAD is detached; cannot bundle a branch".to_string(),
        ));
    }
    let workspace_branch = head.shorthand().unwrap_or_default().to_string();
    let workspace_bundle_ref = format!("refs/heads/{workspace_branch}");
    let workspace_head_sha = head
        .target()
        .map(|oid| oid.to_string())
        .ok_or_else(|| Error::Internal("workspace branch has no target".to_string()))?;
    if workspace_branch != ws.branch {
        tracing::warn!(
            head_branch = %workspace_branch,
            workspace_branch = %ws.branch,
            "transfer bundle: worktree HEAD branch differs from the workspace row; bundling the HEAD branch"
        );
    }

    // 3. Anchor the base commit under a temp ref so the target can fetch it
    //    by name. Best-effort: a missing base just omits the entry.
    let base_oid = resolve_base_commit(&repo, ws);
    let (base_bundle_ref, base_sha) = match base_oid {
        Some(oid) => {
            let base_ref_name = base_bundle_ref(&ws.id.0);
            repo.reference(&base_ref_name, oid, true, "transfer base anchor")
                .map_err(|e| Error::Internal(format!("create base transfer ref failed: {e}")))?;
            temp_refs.push(base_ref_name.clone());
            (Some(base_ref_name), Some(oid.to_string()))
        }
        None => (None, None),
    };

    // 4. Snapshot each sandbox and fetch its branch into a temp ref.
    let mut sandbox_refs = Vec::new();
    for sb in sandboxes {
        let sb_path = PathBuf::from(&sb.path);
        if !sb_path.exists() {
            tracing::warn!(
                agent = %sb.agent_id.0,
                path = %sb.path,
                "transfer bundle: sandbox directory missing; skipping"
            );
            continue;
        }
        // Validate the branch BEFORE snapshotting: a sandbox that can't be
        // bundled (or whose dirty state can't ride the bundled branch) must
        // never be mutated.
        let branch_ref = format!("refs/heads/{}", sb.branch);
        let head_on_branch = {
            let sb_repo = git2::Repository::open(&sb_path)
                .map_err(|e| Error::Internal(format!("open sandbox repo failed: {e}")))?;
            let committish = sb_repo
                .find_reference(&branch_ref)
                .and_then(|r| r.peel_to_commit())
                .is_ok();
            if !committish {
                tracing::warn!(
                    agent = %sb.agent_id.0,
                    branch = %sb.branch,
                    "transfer bundle: sandbox branch missing or unborn; skipping"
                );
                continue;
            }
            sb_repo
                .head()
                .ok()
                .is_some_and(|h| h.is_branch() && h.name().ok() == Some(branch_ref.as_str()))
        };
        // The WIP commit lands on HEAD's branch while the bundler fetches
        // `sb.branch` — so only snapshot when HEAD is on `sb.branch`;
        // otherwise the dirty state would be unreachable from any bundled
        // ref. Diverged HEADs bundle the clean branch tip instead.
        let wip = if head_on_branch {
            let wip = snapshot_wip(&sb_path)?;
            if wip.is_some() {
                snapshotted.push(sb_path.clone());
            }
            wip
        } else {
            tracing::warn!(
                agent = %sb.agent_id.0,
                branch = %sb.branch,
                "transfer bundle: sandbox HEAD is not on its recorded branch; bundling the branch tip without a WIP snapshot"
            );
            None
        };
        let bundle_ref = sandbox_bundle_ref(&ws.id.0, &sb.agent_id.0);
        fetch_local_ref(worktree, &sb_path, &branch_ref, &bundle_ref)?;
        temp_refs.push(bundle_ref.clone());
        let head_sha = repo
            .find_reference(&bundle_ref)
            .ok()
            .and_then(|r| r.target())
            .map(|oid| oid.to_string())
            .ok_or_else(|| {
                Error::Internal(format!(
                    "fetched sandbox transfer ref {bundle_ref} did not resolve"
                ))
            })?;
        sandbox_refs.push(SandboxBundleRef {
            agent_id: sb.agent_id.0.clone(),
            branch: sb.branch.clone(),
            bundle_ref,
            head_sha,
            wip_commit_sha: wip,
            base_commit_sha: sb.base_commit_sha.clone(),
            snapshot_commit_sha: sb.snapshot_commit_sha.clone(),
        });
    }

    // 5. Create and verify the bundle (full history — self-contained, no
    //    prerequisites, so the target can clone/fetch with no other remote).
    let mut ref_args = vec![workspace_bundle_ref.clone()];
    if let Some(r) = &base_bundle_ref {
        ref_args.push(r.clone());
    }
    ref_args.extend(sandbox_refs.iter().map(|s| s.bundle_ref.clone()));
    run_git(worktree, |cmd| {
        cmd.arg("bundle").arg("create").arg(bundle_path);
        cmd.args(&ref_args);
    })
    .map_err(|e| Error::Internal(format!("git bundle create failed: {e}")))?;
    run_git(worktree, |cmd| {
        cmd.arg("bundle").arg("verify").arg(bundle_path);
    })
    .map_err(|e| Error::Internal(format!("git bundle verify failed: {e}")))?;

    // 6. Bundle every unpublished worktree submodule on its own. Runs after
    //    the WIP snapshot so the gitlinks that travel are final; detection
    //    order (by path) puts a parent before its nested children.
    let unpublished = find_unpublished_submodules(worktree)?;
    let mut submodule_refs = Vec::with_capacity(unpublished.len());
    for (index, sub) in unpublished.iter().enumerate() {
        if index == 0 {
            std::fs::create_dir_all(submodules_dir).map_err(|e| {
                Error::Internal(format!("create submodule bundle staging dir failed: {e}"))
            })?;
        }
        let out_path = submodules_dir.join(format!("{index}.bundle"));
        let bundle_entry = submodule_bundle_entry(index);
        // Register before creating so a partial file is cleaned up on failure.
        submodule_bundles.push((out_path.clone(), bundle_entry.clone()));
        let bundle_ref = submodule_bundle_ref(&ws.id.0, index);
        bundle_submodule(sub, &bundle_ref, &out_path)?;
        tracing::info!(
            path = %sub.path,
            commit = %sub.commit_sha,
            entry = %bundle_entry,
            "transfer bundle: bundled unpublished submodule commit"
        );
        submodule_refs.push(SubmoduleBundleRef {
            name: sub.name.clone(),
            path: sub.path.clone(),
            commit_sha: sub.commit_sha.clone(),
            branch: sub.branch.clone(),
            origin_url: sub.origin_url.clone(),
            bundle_entry,
            bundle_ref,
        });
    }

    Ok(TransferRefsManifest {
        workspace_branch,
        workspace_bundle_ref,
        workspace_head_sha,
        workspace_wip_commit_sha: workspace_wip,
        base_ref: ws.base_ref.clone(),
        base_bundle_ref,
        base_sha,
        sandboxes: sandbox_refs,
        submodules: submodule_refs,
    })
}

/// Write one submodule's self-contained bundle: anchor `commit_sha` under
/// `bundle_ref` in the submodule's own repository, `git bundle create` it
/// together with `HEAD` (so a plain clone from the bundle has something to
/// check out), `verify`, and delete the temp ref again — success or failure.
fn bundle_submodule(sub: &UnpublishedSubmodule, bundle_ref: &str, out_path: &Path) -> Result<()> {
    let repo = git2::Repository::open(&sub.repo_dir)
        .map_err(|e| Error::Internal(format!("open submodule {} failed: {e}", sub.path)))?;
    let oid = git2::Oid::from_str(&sub.commit_sha)
        .map_err(|e| Error::Internal(format!("submodule {} commit sha: {e}", sub.path)))?;
    repo.reference(bundle_ref, oid, true, "transfer submodule anchor")
        .map_err(|e| {
            Error::Internal(format!(
                "create submodule transfer ref for {} failed: {e}",
                sub.path
            ))
        })?;
    let result = run_git(&sub.repo_dir, |cmd| {
        cmd.arg("bundle")
            .arg("create")
            .arg(out_path)
            .arg(bundle_ref)
            .arg("HEAD");
    })
    .map_err(|e| {
        Error::Internal(format!(
            "git bundle create for submodule {} failed: {e}",
            sub.path
        ))
    })
    .and_then(|()| {
        run_git(&sub.repo_dir, |cmd| {
            cmd.arg("bundle").arg("verify").arg(out_path);
        })
        .map_err(|e| {
            Error::Internal(format!(
                "git bundle verify for submodule {} failed: {e}",
                sub.path
            ))
        })
    });
    if let Ok(mut r) = repo.find_reference(bundle_ref) {
        let _ = r.delete();
    }
    result
}

/// Resolve the workspace base commit locally: the remote-tracking ref for
/// `baseRef` first (matches `provision_worktree`'s preference), then the
/// local branch, then the recorded `baseCommitSha` if the object exists.
fn resolve_base_commit(repo: &git2::Repository, ws: &Workspace) -> Option<git2::Oid> {
    if let Some(base) = ws.base_ref.as_deref() {
        for candidate in [
            format!("refs/remotes/origin/{base}"),
            format!("refs/heads/{base}"),
        ] {
            if let Some(oid) = repo
                .find_reference(&candidate)
                .and_then(|r| r.peel_to_commit())
                .ok()
                .map(|c| c.id())
            {
                return Some(oid);
            }
        }
    }
    if let Some(sha) = ws.base_commit_sha.as_deref() {
        if let Ok(oid) = git2::Oid::from_str(sha) {
            if repo.find_commit(oid).is_ok() {
                return Some(oid);
            }
        }
    }
    None
}

/// Fetch `src_ref` from a local repository into `dst_ref` of the worktree
/// repo. Shells out with an explicit full refspec and tag auto-follow
/// disabled, for the same reason as the sandbox merge path: `CoW` clones carry
/// non-commit refs (`refs/intent/blobs/*`, `refs/stash`) that libgit2's local
/// transport trips over.
fn fetch_local_ref(worktree: &Path, from_repo: &Path, src_ref: &str, dst_ref: &str) -> Result<()> {
    let from = from_repo
        .to_str()
        .ok_or_else(|| Error::Internal("sandbox path not UTF-8".to_string()))?;
    let refspec = format!("+{src_ref}:{dst_ref}");
    run_git(worktree, |cmd| {
        cmd.arg("fetch")
            .arg("--no-tags")
            .arg("--quiet")
            .arg(from)
            .arg(&refspec);
    })
    .map_err(|e| Error::Internal(format!("fetch sandbox branch failed: {e}")))
}

/// Run a git subcommand in `dir` with prompts disabled; returns the trimmed
/// stderr as the error string on non-zero exit.
pub(crate) fn run_git(
    dir: &Path,
    configure: impl FnOnce(&mut Command),
) -> std::result::Result<(), String> {
    let mut cmd = Command::new("git");
    configure(&mut cmd);
    let out = cmd
        .current_dir(dir)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(std::process::Stdio::null())
        .output()
        .map_err(|e| e.to_string())?;
    if out.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
    }
}

/// Delete the temporary transfer refs from the worktree repo (best-effort).
fn cleanup_temp_refs(worktree: &Path, refs: &[String]) {
    if refs.is_empty() {
        return;
    }
    let Ok(repo) = git2::Repository::open(worktree) else {
        return;
    };
    for name in refs {
        if let Ok(mut r) = repo.find_reference(name) {
            let _ = r.delete();
        }
    }
}

/// Extract the pre-snapshot index tree OID from a WIP commit message.
fn parse_index_tree_trailer(message: &str) -> Option<git2::Oid> {
    message
        .lines()
        .find_map(|line| line.strip_prefix(INDEX_TREE_TRAILER))
        .and_then(|v| git2::Oid::from_str(v.trim()).ok())
}

/// Resolve a commit signature, falling back to a stable default identity when
/// the user has no `user.name`/`user.email` configured (parity with the
/// `sandbox_ops` helper of the same name).
fn resolve_signature(repo: &git2::Repository) -> Result<git2::Signature<'static>> {
    match repo.signature() {
        Ok(sig) => Ok(sig),
        Err(e) if e.code() == git2::ErrorCode::NotFound => {
            git2::Signature::now("Intent", "intent@localhost")
                .map_err(|e| Error::Internal(format!("construct fallback signature failed: {e}")))
        }
        Err(e) => Err(Error::Internal(format!(
            "resolve git signature failed: {e}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{AgentId, WorkspaceId, WorkspaceStatus};
    use intent_store::SandboxStatus;
    use std::fs;

    fn now_iso() -> String {
        intent_core::now_iso()
    }

    /// Init a repo on branch `main` with one committed file.
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

    fn temp_repo(name: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::TempDir::new().unwrap();
        let repo_path = dir.path().join(name);
        init_repo(&repo_path);
        (dir, repo_path)
    }

    /// Stage and commit a single file; returns the commit SHA.
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

    fn head_sha(repo_path: &Path) -> String {
        let repo = git2::Repository::open(repo_path).unwrap();
        let sha = repo.head().unwrap().target().unwrap().to_string();
        sha
    }

    /// Status entries as (path, staged, `wt_modified`, untracked) tuples,
    /// sorted, for exact before/after comparisons.
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
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
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
            base_commit_sha: head_sha(path),
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

    /// Refs listed in a bundle header, via `git bundle list-heads`.
    fn bundle_refs(cwd: &Path, bundle: &Path) -> Vec<String> {
        let out = Command::new("git")
            .arg("bundle")
            .arg("list-heads")
            .arg(bundle)
            .current_dir(cwd)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|l| {
                l.split_whitespace()
                    .nth(1)
                    .map(std::string::ToString::to_string)
            })
            .collect()
    }

    fn repo_ref_names(repo_path: &Path) -> Vec<String> {
        let repo = git2::Repository::open(repo_path).unwrap();
        let names: Vec<String> = repo
            .references()
            .unwrap()
            .names()
            .map(|n| n.unwrap().to_string())
            .collect();
        names
    }

    #[test]
    fn snapshot_wip_clean_repo_is_noop() {
        let (_dir, repo) = temp_repo("clean");
        assert_eq!(snapshot_wip(&repo).unwrap(), None);
        assert!(
            !unwind_wip(&repo).unwrap(),
            "nothing to unwind on clean repo"
        );
    }

    #[test]
    fn snapshot_and_unwind_roundtrip_preserves_staged_unstaged_untracked() {
        let (_dir, repo) = temp_repo("dirty");
        let base = head_sha(&repo);

        // staged change
        fs::write(repo.join("staged.txt"), "staged\n").unwrap();
        {
            let r = git2::Repository::open(&repo).unwrap();
            let mut index = r.index().unwrap();
            index.add_path(Path::new("staged.txt")).unwrap();
            index.write().unwrap();
        }
        // unstaged modification of a tracked file
        fs::write(repo.join("README.md"), "modified\n").unwrap();
        // untracked file
        fs::write(repo.join("untracked.txt"), "untracked\n").unwrap();

        let before = status_fingerprint(&repo);
        let wip = snapshot_wip(&repo).unwrap().expect("dirty repo snapshots");
        assert_eq!(head_sha(&repo), wip);
        {
            let r = git2::Repository::open(&repo).unwrap();
            let head = r.head().unwrap().peel_to_commit().unwrap();
            assert!(head.message().unwrap().starts_with(TRANSFER_WIP_SENTINEL));
        }

        assert!(unwind_wip(&repo).unwrap());
        assert_eq!(
            head_sha(&repo),
            base,
            "HEAD back at the pre-snapshot commit"
        );
        assert_eq!(
            status_fingerprint(&repo),
            before,
            "staged/unstaged/untracked split restored exactly"
        );
        // File contents intact.
        assert_eq!(
            fs::read_to_string(repo.join("README.md")).unwrap(),
            "modified\n"
        );
        assert_eq!(
            fs::read_to_string(repo.join("untracked.txt")).unwrap(),
            "untracked\n"
        );
    }

    #[test]
    fn unwind_wip_ignores_ordinary_commits() {
        let (_dir, repo) = temp_repo("ordinary");
        commit_file(&repo, "a.txt", "a\n", "feat: ordinary commit");
        let head = head_sha(&repo);
        assert!(!unwind_wip(&repo).unwrap());
        assert_eq!(head_sha(&repo), head);
    }

    #[test]
    fn bundle_clean_workspace_contains_branch_and_cleans_temp_refs() {
        let (dir, repo) = temp_repo("ws-clean");
        let ws = workspace_for_repo(&repo);
        let staging = dir.path().join("staging");

        let TransferBundle {
            bundle_path,
            submodule_bundles,
            refs: manifest,
        } = create_transfer_bundle(&ws, &[], &staging).unwrap();
        assert!(bundle_path.exists());
        assert!(submodule_bundles.is_empty());
        assert!(manifest.submodules.is_empty());
        assert_eq!(manifest.workspace_branch, "main");
        assert_eq!(manifest.workspace_bundle_ref, "refs/heads/main");
        assert_eq!(manifest.workspace_head_sha, head_sha(&repo));
        assert_eq!(manifest.workspace_wip_commit_sha, None);
        assert!(manifest.sandboxes.is_empty());

        let refs = bundle_refs(&repo, &bundle_path);
        assert!(refs.contains(&"refs/heads/main".to_string()), "{refs:?}");

        // Clone from the bundle and check the file arrived.
        let clone_dst = dir.path().join("from-bundle");
        let out = Command::new("git")
            .arg("clone")
            .arg("--quiet")
            .arg("-b")
            .arg("main")
            .arg(&bundle_path)
            .arg(&clone_dst)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            fs::read_to_string(clone_dst.join("README.md")).unwrap(),
            "hello\n"
        );

        // No temporary transfer refs left behind.
        assert!(
            repo_ref_names(&repo)
                .iter()
                .all(|r| !r.starts_with(TRANSFER_REF_NS)),
            "temp refs cleaned up"
        );
    }

    #[test]
    fn bundle_dirty_workspace_snapshots_wip_and_unwinds() {
        let (dir, repo) = temp_repo("ws-dirty");
        let base = head_sha(&repo);
        fs::write(repo.join("wip.txt"), "wip\n").unwrap();
        let before = status_fingerprint(&repo);

        let ws = workspace_for_repo(&repo);
        let TransferBundle {
            bundle_path,
            refs: manifest,
            ..
        } = create_transfer_bundle(&ws, &[], &dir.path().join("staging")).unwrap();
        let wip = manifest
            .workspace_wip_commit_sha
            .clone()
            .expect("dirty worktree produces a WIP commit");
        assert_eq!(manifest.workspace_head_sha, wip);
        assert_eq!(head_sha(&repo), wip, "WIP left in place on success");
        assert!(bundle_path.exists());

        // The caller unwinds after the export settles.
        assert!(unwind_wip(&repo).unwrap());
        assert_eq!(head_sha(&repo), base);
        assert_eq!(status_fingerprint(&repo), before);
    }

    #[test]
    fn bundle_includes_base_and_sandbox_branches() {
        let (dir, repo) = temp_repo("ws-sb");
        let ws = {
            let mut ws = workspace_for_repo(&repo);
            ws.base_ref = Some("main".to_string());
            ws
        };

        // Sandbox with a committed change plus a dirty file.
        let agent = AgentId::new();
        let branch = format!("sb/{}", agent.0);
        let sb_path = dir.path().join("sandbox");
        make_sandbox_clone(&repo, &sb_path, &branch);
        commit_file(&sb_path, "sb.txt", "sandbox work\n", "feat: sandbox commit");
        fs::write(sb_path.join("sb-wip.txt"), "sandbox wip\n").unwrap();
        let sb = sandbox_row(&ws, &agent, &sb_path, &branch);

        let TransferBundle {
            bundle_path,
            refs: manifest,
            ..
        } = create_transfer_bundle(&ws, &[sb], &dir.path().join("staging")).unwrap();

        assert_eq!(manifest.base_ref.as_deref(), Some("main"));
        assert_eq!(
            manifest.base_bundle_ref.as_deref(),
            Some(base_bundle_ref(&ws.id.0).as_str())
        );
        assert_eq!(manifest.base_sha.as_deref(), Some(head_sha(&repo).as_str()));

        assert_eq!(manifest.sandboxes.len(), 1);
        let entry = &manifest.sandboxes[0];
        assert_eq!(entry.agent_id, agent.0);
        assert_eq!(entry.branch, branch);
        assert_eq!(entry.bundle_ref, sandbox_bundle_ref(&ws.id.0, &agent.0));
        let sb_wip = entry
            .wip_commit_sha
            .clone()
            .expect("dirty sandbox snapshots");
        assert_eq!(entry.head_sha, sb_wip);
        assert_eq!(head_sha(&sb_path), sb_wip);

        let refs = bundle_refs(&repo, &bundle_path);
        assert!(refs.contains(&"refs/heads/main".to_string()), "{refs:?}");
        assert!(refs.contains(&base_bundle_ref(&ws.id.0)), "{refs:?}");
        assert!(refs.contains(&entry.bundle_ref), "{refs:?}");

        // The sandbox WIP content is reachable from the bundle: fetch the
        // sandbox ref into a fresh clone and check the dirty file arrived.
        let clone_dst = dir.path().join("from-bundle");
        let out = Command::new("git")
            .arg("clone")
            .arg("--quiet")
            .arg("-b")
            .arg("main")
            .arg(&bundle_path)
            .arg(&clone_dst)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let out = Command::new("git")
            .arg("fetch")
            .arg("--quiet")
            .arg(bundle_path.to_str().unwrap())
            .arg(format!("{}:refs/heads/{branch}", entry.bundle_ref))
            .current_dir(&clone_dst)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let cloned = git2::Repository::open(&clone_dst).unwrap();
        let tip = cloned
            .find_reference(&format!("refs/heads/{branch}"))
            .unwrap()
            .peel_to_commit()
            .unwrap();
        assert_eq!(tip.id().to_string(), sb_wip);
        assert!(tip.tree().unwrap().get_name("sb-wip.txt").is_some());

        // Temp refs cleaned from the source worktree.
        assert!(
            repo_ref_names(&repo)
                .iter()
                .all(|r| !r.starts_with(TRANSFER_REF_NS)),
            "temp refs cleaned up"
        );
        // Sandbox WIP left in place on success for the caller to unwind.
        assert!(unwind_wip(&sb_path).unwrap());
    }

    #[test]
    fn snapshot_and_unwind_roundtrip_preserves_deletions() {
        let (_dir, repo) = temp_repo("deletions");
        commit_file(&repo, "staged-del.txt", "to delete\n", "feat: add files");
        commit_file(&repo, "wt-del.txt", "to delete\n", "feat: add more");
        let base = head_sha(&repo);

        // staged deletion
        {
            let r = git2::Repository::open(&repo).unwrap();
            let mut index = r.index().unwrap();
            index.remove_path(Path::new("staged-del.txt")).unwrap();
            index.write().unwrap();
        }
        fs::remove_file(repo.join("staged-del.txt")).unwrap();
        // unstaged deletion of a tracked file
        fs::remove_file(repo.join("wt-del.txt")).unwrap();

        let before = status_fingerprint(&repo);
        let wip = snapshot_wip(&repo).unwrap().expect("dirty repo snapshots");
        // Deletions are captured: neither file is in the WIP tree.
        {
            let r = git2::Repository::open(&repo).unwrap();
            let tree = r
                .find_commit(git2::Oid::from_str(&wip).unwrap())
                .unwrap()
                .tree()
                .unwrap();
            assert!(tree.get_name("staged-del.txt").is_none());
            assert!(tree.get_name("wt-del.txt").is_none());
        }

        assert!(unwind_wip(&repo).unwrap());
        assert_eq!(head_sha(&repo), base);
        assert_eq!(
            status_fingerprint(&repo),
            before,
            "staged/unstaged deletion split restored exactly"
        );
        assert!(!repo.join("staged-del.txt").exists());
        assert!(!repo.join("wt-del.txt").exists());
    }

    #[test]
    fn bundle_skips_missing_sandbox_directory() {
        let (dir, repo) = temp_repo("ws-missing-sb");
        let ws = workspace_for_repo(&repo);
        let agent = AgentId::new();
        let sb = sandbox_row(&ws, &agent, &repo, &format!("sb/{}", agent.0));
        let mut missing = sb.clone();
        missing.path = dir.path().join("gone").to_string_lossy().to_string();

        let TransferBundle { refs: manifest, .. } =
            create_transfer_bundle(&ws, &[missing], &dir.path().join("staging")).unwrap();
        assert!(manifest.sandboxes.is_empty(), "missing sandbox skipped");
    }

    #[test]
    fn bundle_never_mutates_dirty_sandbox_with_missing_branch() {
        let (dir, repo) = temp_repo("ws-sb-nobranch");
        let ws = workspace_for_repo(&repo);

        // Dirty sandbox whose recorded branch does not exist.
        let agent = AgentId::new();
        let sb_path = dir.path().join("sandbox");
        make_sandbox_clone(&repo, &sb_path, &format!("sb/{}", agent.0));
        fs::write(sb_path.join("dirty.txt"), "dirty\n").unwrap();
        let sb_head = head_sha(&sb_path);
        let before = status_fingerprint(&sb_path);
        let mut sb = sandbox_row(&ws, &agent, &sb_path, &format!("sb/{}", agent.0));
        sb.branch = "sb/does-not-exist".to_string();

        let TransferBundle { refs: manifest, .. } =
            create_transfer_bundle(&ws, &[sb], &dir.path().join("staging")).unwrap();
        assert!(manifest.sandboxes.is_empty(), "unbundlable sandbox skipped");
        // The skipped sandbox was not touched: no WIP commit, dirty state intact.
        assert_eq!(
            head_sha(&sb_path),
            sb_head,
            "no WIP commit on skipped sandbox"
        );
        assert_eq!(status_fingerprint(&sb_path), before);
    }

    #[test]
    fn bundle_diverged_sandbox_head_bundles_branch_tip_without_wip() {
        let (dir, repo) = temp_repo("ws-sb-diverged");
        let ws = workspace_for_repo(&repo);

        // Sandbox whose branch exists but HEAD sits on another branch.
        let agent = AgentId::new();
        let branch = format!("sb/{}", agent.0);
        let sb_path = dir.path().join("sandbox");
        make_sandbox_clone(&repo, &sb_path, &branch);
        let branch_tip = head_sha(&sb_path);
        {
            let r = git2::Repository::open(&sb_path).unwrap();
            let head = r.head().unwrap().peel_to_commit().unwrap();
            r.branch("other", &head, false).unwrap();
            r.set_head("refs/heads/other").unwrap();
        }
        fs::write(sb_path.join("dirty.txt"), "dirty\n").unwrap();
        let before = status_fingerprint(&sb_path);
        let sb = sandbox_row(&ws, &agent, &sb_path, &branch);

        let TransferBundle { refs: manifest, .. } =
            create_transfer_bundle(&ws, &[sb], &dir.path().join("staging")).unwrap();
        assert_eq!(manifest.sandboxes.len(), 1);
        let entry = &manifest.sandboxes[0];
        assert_eq!(entry.wip_commit_sha, None, "no WIP on diverged HEAD");
        assert_eq!(entry.head_sha, branch_tip, "clean branch tip bundled");
        // Sandbox untouched: dirty state stays on the other branch.
        assert_eq!(status_fingerprint(&sb_path), before);
    }

    /// Untracked nested repo like agents leave behind: a directory with its
    /// own real `.git` directory and a commit.
    fn make_nested_repo(parent: &Path, name: &str) -> PathBuf {
        let nested = parent.join(name);
        init_repo(&nested);
        nested
    }

    /// Worktree-style nested checkout: `git worktree add` creates a directory
    /// whose `.git` is a FILE pointing at the parent repo's worktree metadata.
    fn make_nested_worktree(repo: &Path, name: &str) -> PathBuf {
        run_git(repo, |cmd| {
            cmd.args(["worktree", "add", "-b"])
                .arg(format!("wt-{}", name.trim_start_matches('.')))
                .arg(name);
        })
        .unwrap();
        repo.join(name)
    }

    #[test]
    fn snapshot_wip_skips_nested_repos_and_worktrees() {
        let (_dir, repo) = temp_repo("nested");
        // Untracked nested git repo (real `.git` dir) and a worktree-style
        // checkout (`.git` FILE), alongside ordinary dirty state.
        make_nested_repo(&repo, ".import-wt");
        make_nested_worktree(&repo, ".roundtrip-wt");
        fs::write(repo.join("staged.txt"), "staged\n").unwrap();
        {
            let r = git2::Repository::open(&repo).unwrap();
            let mut index = r.index().unwrap();
            index.add_path(Path::new("staged.txt")).unwrap();
            index.write().unwrap();
        }
        fs::write(repo.join("README.md"), "modified\n").unwrap();
        fs::write(repo.join("untracked.txt"), "untracked\n").unwrap();

        let base = head_sha(&repo);
        let before = status_fingerprint(&repo);
        let wip = snapshot_wip(&repo)
            .expect("snapshot succeeds despite nested repos")
            .expect("dirty repo snapshots");

        // The WIP tree carries the ordinary dirty files but not the nested
        // repos (no gitlink entries either).
        {
            let r = git2::Repository::open(&repo).unwrap();
            let tree = r
                .find_commit(git2::Oid::from_str(&wip).unwrap())
                .unwrap()
                .tree()
                .unwrap();
            assert!(tree.get_name("staged.txt").is_some());
            assert!(tree.get_name("untracked.txt").is_some());
            assert!(
                tree.get_name(".import-wt").is_none(),
                "nested repo not in WIP tree"
            );
            assert!(
                tree.get_name(".roundtrip-wt").is_none(),
                "nested worktree not in WIP tree"
            );
        }

        assert!(unwind_wip(&repo).unwrap());
        assert_eq!(head_sha(&repo), base);
        assert_eq!(
            status_fingerprint(&repo),
            before,
            "exact pre-snapshot status restored"
        );
        // Nested dirs untouched on disk.
        assert!(repo.join(".import-wt/.git").is_dir());
        assert_eq!(
            fs::read_to_string(repo.join(".import-wt/README.md")).unwrap(),
            "hello\n"
        );
        assert!(
            repo.join(".roundtrip-wt/.git").is_file(),
            "worktree .git file intact"
        );
        assert_eq!(
            fs::read_to_string(repo.join(".roundtrip-wt/README.md")).unwrap(),
            "hello\n"
        );
    }

    /// Field shape (`.worktrees/<name>` regression): the nested repo is not
    /// at the workdir root but one level down inside an UNTRACKED parent
    /// directory. Status reports the nested dir itself (recursion stops at
    /// the embedded repo), but staging must still skip it without erroring
    /// and without dragging the parent dir into the WIP tree.
    #[test]
    fn snapshot_wip_skips_nested_repo_inside_untracked_parent_dir() {
        let (_dir, repo) = temp_repo("nested-parent");
        make_nested_repo(&repo, ".worktrees/inner");
        fs::write(repo.join("wip.txt"), "wip\n").unwrap();

        let base = head_sha(&repo);
        let before = status_fingerprint(&repo);
        let wip = snapshot_wip(&repo)
            .expect("snapshot succeeds despite nested repo under untracked parent")
            .expect("dirty repo snapshots");

        {
            let r = git2::Repository::open(&repo).unwrap();
            let tree = r
                .find_commit(git2::Oid::from_str(&wip).unwrap())
                .unwrap()
                .tree()
                .unwrap();
            assert!(tree.get_name("wip.txt").is_some());
            assert!(
                tree.get_name(".worktrees").is_none(),
                "untracked parent of a nested repo not in WIP tree"
            );
        }

        assert!(unwind_wip(&repo).unwrap());
        assert_eq!(head_sha(&repo), base);
        assert_eq!(
            status_fingerprint(&repo),
            before,
            "exact pre-snapshot status restored"
        );
        assert!(repo.join(".worktrees/inner/.git").is_dir());
        assert_eq!(
            fs::read_to_string(repo.join(".worktrees/inner/README.md")).unwrap(),
            "hello\n"
        );
    }

    /// Exact field shape: `.worktrees/<name>` is a linked worktree of a
    /// SUBMODULE of the outer repo — its `.git` is a file whose gitdir
    /// points into `<outer>/.git/modules/<sub>/worktrees/<name>` — sitting
    /// inside the untracked `.worktrees/` parent dir.
    #[test]
    fn snapshot_wip_skips_submodule_worktree_inside_untracked_parent_dir() {
        let (dir, repo) = temp_repo("field-shape");
        // Real submodule: checkout on disk, gitdir under `.git/modules/sub`.
        let sub_src = dir.path().join("sub-src");
        init_repo(&sub_src);
        run_git(&repo, |cmd| {
            cmd.args(["-c", "protocol.file.allow=always", "submodule", "add"])
                .arg(sub_src.to_str().unwrap())
                .arg("sub");
        })
        .unwrap();
        run_git(&repo, |cmd| {
            cmd.args([
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.com",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-m",
                "add submodule",
            ]);
        })
        .unwrap();

        // Linked worktree of the submodule inside the outer workdir.
        let wt = repo.join(".worktrees/cloudlands-fe-compact-wake-header");
        run_git(&repo.join("sub"), |cmd| {
            cmd.args(["worktree", "add", "-b", "wt-field"]).arg(&wt);
        })
        .unwrap();
        assert!(
            wt.join(".git").is_file(),
            "submodule worktree .git is a gitdir file"
        );
        let gitdir = fs::read_to_string(wt.join(".git")).unwrap();
        assert!(
            gitdir.contains("modules/sub/worktrees"),
            "gitdir points into the outer repo's submodule metadata: {gitdir}"
        );

        fs::write(repo.join("wip.txt"), "wip\n").unwrap();
        let base = head_sha(&repo);
        let before = status_fingerprint(&repo);
        let wip = snapshot_wip(&repo)
            .expect("snapshot succeeds despite submodule worktree under untracked parent")
            .expect("dirty repo snapshots");

        {
            let r = git2::Repository::open(&repo).unwrap();
            let tree = r
                .find_commit(git2::Oid::from_str(&wip).unwrap())
                .unwrap()
                .tree()
                .unwrap();
            assert!(tree.get_name("wip.txt").is_some());
            assert!(
                tree.get_name(".worktrees").is_none(),
                "untracked parent of a submodule worktree not in WIP tree"
            );
        }

        assert!(unwind_wip(&repo).unwrap());
        assert_eq!(head_sha(&repo), base);
        assert_eq!(
            status_fingerprint(&repo),
            before,
            "exact pre-snapshot status restored"
        );
        assert!(wt.join(".git").is_file(), "worktree .git file intact");
        assert_eq!(fs::read_to_string(wt.join("README.md")).unwrap(), "hello\n");
    }

    /// Parent dir ignored via `.git/info/exclude`: the nested repo inside it
    /// must neither travel in the WIP tree nor make the snapshot error.
    #[test]
    fn snapshot_wip_skips_nested_repo_under_excluded_parent_dir() {
        let (_dir, repo) = temp_repo("excluded-parent");
        fs::create_dir_all(repo.join(".git/info")).unwrap();
        fs::write(repo.join(".git/info/exclude"), ".worktrees/\n").unwrap();
        make_nested_repo(&repo, ".worktrees/inner");
        fs::write(repo.join("wip.txt"), "wip\n").unwrap();

        let base = head_sha(&repo);
        let before = status_fingerprint(&repo);
        let wip = snapshot_wip(&repo)
            .expect("snapshot succeeds despite nested repo under excluded parent")
            .expect("dirty repo snapshots");

        {
            let r = git2::Repository::open(&repo).unwrap();
            let tree = r
                .find_commit(git2::Oid::from_str(&wip).unwrap())
                .unwrap()
                .tree()
                .unwrap();
            assert!(tree.get_name("wip.txt").is_some());
            assert!(
                tree.get_name(".worktrees").is_none(),
                "excluded parent dir not in WIP tree"
            );
        }

        assert!(unwind_wip(&repo).unwrap());
        assert_eq!(head_sha(&repo), base);
        assert_eq!(
            status_fingerprint(&repo),
            before,
            "exact pre-snapshot status restored"
        );
        assert!(repo.join(".worktrees/inner/.git").is_dir());
        assert_eq!(
            fs::read_to_string(repo.join(".worktrees/inner/README.md")).unwrap(),
            "hello\n"
        );
    }

    /// A tracked submodule staged for removal while its checkout remains on
    /// disk (`INDEX_DELETED` + `WT_NEW`) is real dirt: the staged deletion
    /// must travel in the WIP commit, while `add_all` still skips re-adding the
    /// on-disk checkout (which would fail or undo the removal).
    #[test]
    fn staged_submodule_removal_still_counts_as_dirty() {
        let (_dir, repo) = temp_repo("sub-rm");
        make_nested_repo(&repo, "vendor");
        // Record the nested repo as a gitlink, commit, then stage its removal
        // leaving the checkout on disk.
        run_git(&repo, |cmd| {
            cmd.args(["add", "vendor"]);
        })
        .unwrap();
        run_git(&repo, |cmd| {
            cmd.args([
                "-c",
                "user.name=Test",
                "-c",
                "user.email=test@example.com",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-m",
                "add gitlink",
            ]);
        })
        .unwrap();
        run_git(&repo, |cmd| {
            cmd.args(["rm", "--cached", "vendor"]);
        })
        .unwrap();

        let base = head_sha(&repo);
        let before = status_fingerprint(&repo);
        let wip = snapshot_wip(&repo)
            .expect("snapshot succeeds")
            .expect("staged submodule removal is dirt, not an ignorable nested repo");
        {
            let r = git2::Repository::open(&repo).unwrap();
            let tree = r
                .find_commit(git2::Oid::from_str(&wip).unwrap())
                .unwrap()
                .tree()
                .unwrap();
            assert!(
                tree.get_name("vendor").is_none(),
                "WIP tree captures the staged removal"
            );
        }

        assert!(unwind_wip(&repo).unwrap());
        assert_eq!(head_sha(&repo), base);
        assert_eq!(status_fingerprint(&repo), before);
        assert!(
            repo.join("vendor/.git").is_dir(),
            "checkout untouched on disk"
        );
    }

    /// An untracked symlink pointing at a directory that contains `.git` is
    /// NOT a nested repo — git stages it as a symlink blob — so it must count
    /// as dirt and travel in the WIP commit.
    #[cfg(unix)]
    #[test]
    fn untracked_symlink_to_repo_dir_is_dirt_not_nested_repo() {
        let (dir, repo) = temp_repo("symlink");
        // Target repo lives OUTSIDE the workdir; only the symlink is inside.
        let target = make_nested_repo(dir.path(), "target-repo");
        std::os::unix::fs::symlink(&target, repo.join("link")).unwrap();

        let base = head_sha(&repo);
        let before = status_fingerprint(&repo);
        let wip = snapshot_wip(&repo)
            .expect("snapshot succeeds")
            .expect("untracked symlink is dirt");
        {
            let r = git2::Repository::open(&repo).unwrap();
            let tree = r
                .find_commit(git2::Oid::from_str(&wip).unwrap())
                .unwrap()
                .tree()
                .unwrap();
            let entry = tree.get_name("link").expect("symlink travels in WIP");
            assert_eq!(
                entry.filemode(),
                i32::from(git2::FileMode::Link),
                "staged as a symlink blob, not a gitlink"
            );
        }

        assert!(unwind_wip(&repo).unwrap());
        assert_eq!(head_sha(&repo), base);
        assert_eq!(status_fingerprint(&repo), before);
        assert!(repo.join("link").exists(), "symlink intact on disk");
    }

    #[test]
    fn nested_repo_as_only_anomaly_is_not_dirty() {
        let (_dir, repo) = temp_repo("nested-only");
        make_nested_repo(&repo, ".import-wt");
        let base = head_sha(&repo);
        assert_eq!(
            snapshot_wip(&repo).unwrap(),
            None,
            "a nested repo alone must not force a WIP commit"
        );
        assert_eq!(head_sha(&repo), base, "no WIP commit created");
    }

    #[test]
    fn bundle_workspace_with_nested_repo_succeeds() {
        let (dir, repo) = temp_repo("ws-nested");
        make_nested_repo(&repo, ".import-wt");
        fs::write(repo.join("wip.txt"), "wip\n").unwrap();
        let base = head_sha(&repo);
        let before = status_fingerprint(&repo);

        let ws = workspace_for_repo(&repo);
        let TransferBundle {
            bundle_path,
            refs: manifest,
            ..
        } = create_transfer_bundle(&ws, &[], &dir.path().join("staging")).unwrap();
        assert!(bundle_path.exists());
        let wip = manifest
            .workspace_wip_commit_sha
            .clone()
            .expect("dirty worktree produces a WIP commit");
        {
            let r = git2::Repository::open(&repo).unwrap();
            let tree = r
                .find_commit(git2::Oid::from_str(&wip).unwrap())
                .unwrap()
                .tree()
                .unwrap();
            assert!(tree.get_name("wip.txt").is_some());
            assert!(
                tree.get_name(".import-wt").is_none(),
                "nested repo not in bundled WIP tree"
            );
        }

        assert!(unwind_wip(&repo).unwrap());
        assert_eq!(head_sha(&repo), base);
        assert_eq!(status_fingerprint(&repo), before);
        assert!(repo.join(".import-wt/.git").is_dir(), "nested repo intact");
        assert_eq!(
            fs::read_to_string(repo.join(".import-wt/README.md")).unwrap(),
            "hello\n"
        );
    }

    #[test]
    fn bundle_failure_restores_source_exactly() {
        let (dir, repo) = temp_repo("ws-fail");
        let base = head_sha(&repo);
        fs::write(repo.join("wip.txt"), "wip\n").unwrap();
        let before = status_fingerprint(&repo);

        // A sandbox whose path exists but is not a git repo forces a failure
        // AFTER the workspace WIP snapshot has been taken.
        let bogus = dir.path().join("not-a-repo");
        fs::create_dir_all(&bogus).unwrap();
        let ws = workspace_for_repo(&repo);
        let agent = AgentId::new();
        let mut sb = sandbox_row(&ws, &agent, &repo, &format!("sb/{}", agent.0));
        sb.path = bogus.to_string_lossy().to_string();

        let staging = dir.path().join("staging");
        let err = create_transfer_bundle(&ws, &[sb], &staging);
        assert!(err.is_err());

        // Worktree restored exactly as found; no bundle, no temp refs.
        assert_eq!(head_sha(&repo), base, "WIP snapshot unwound on failure");
        assert_eq!(status_fingerprint(&repo), before);
        assert!(!staging.join(format!("{}.bundle", ws.id.0)).exists());
        assert!(
            repo_ref_names(&repo)
                .iter()
                .all(|r| !r.starts_with(TRANSFER_REF_NS)),
            "temp refs cleaned up"
        );
    }

    /// A tracked submodule with a local-only commit gets its own
    /// self-contained bundle (listed in `refs.submodules`, cloneable at the
    /// recorded sha) and leaves no temp ref behind in the submodule repo; a
    /// published submodule produces no bundle at all.
    #[test]
    fn bundle_includes_unpublished_submodules() {
        use crate::transfer_submodules::test_fixture::{
            git, local_commit, superproject_with_submodule,
        };
        let dir = tempfile::TempDir::new().unwrap();
        let (sup, origin) = superproject_with_submodule(dir.path());
        let sub = sup.join("sub");
        git(&sub, &["checkout", "-q", "main"]);
        let sha = local_commit(&sub, "wip.txt");
        let ws = workspace_for_repo(&sup);
        let staging = dir.path().join("staging");

        let bundle = create_transfer_bundle(&ws, &[], &staging).unwrap();
        assert_eq!(bundle.submodule_bundles.len(), 1, "{bundle:?}");
        let (sub_bundle, entry) = &bundle.submodule_bundles[0];
        assert_eq!(entry, "git/submodules/0.bundle");
        assert_eq!(sub_bundle, &staging.join("submodules/0.bundle"));
        assert!(sub_bundle.exists());
        assert_eq!(bundle.refs.submodules.len(), 1);
        let s = &bundle.refs.submodules[0];
        assert_eq!(s.name, "sub");
        assert_eq!(s.path, "sub");
        assert_eq!(s.commit_sha, sha);
        assert_eq!(s.branch.as_deref(), Some("main"));
        assert_eq!(
            s.origin_url.as_deref().map(|u| u.trim_end_matches('/')),
            Some(origin.to_str().unwrap())
        );
        assert_eq!(s.bundle_entry, *entry);
        assert_eq!(s.bundle_ref, submodule_bundle_ref(&ws.id.0, 0));

        // The bundle carries the anchor ref and is cloneable at the sha.
        let refs = bundle_refs(&sub, sub_bundle);
        assert!(refs.contains(&s.bundle_ref), "{refs:?}");
        let clone_dst = dir.path().join("sub-from-bundle");
        git(
            dir.path(),
            &[
                "clone",
                "-q",
                sub_bundle.to_str().unwrap(),
                clone_dst.to_str().unwrap(),
            ],
        );
        assert_eq!(git(&clone_dst, &["rev-parse", "HEAD"]), sha);
        assert!(clone_dst.join("wip.txt").exists());

        // No temp refs left in the submodule or superproject repos.
        assert!(
            repo_ref_names(&sub)
                .iter()
                .chain(repo_ref_names(&sup).iter())
                .all(|r| !r.starts_with(TRANSFER_REF_NS)),
            "temp refs cleaned up"
        );

        // Once pushed, the submodule is published: nothing to bundle.
        git(&sub, &["push", "-q", "origin", "main"]);
        let staging2 = dir.path().join("staging2");
        let bundle = create_transfer_bundle(&ws, &[], &staging2).unwrap();
        assert!(bundle.submodule_bundles.is_empty());
        assert!(bundle.refs.submodules.is_empty());
        assert!(!staging2.join("submodules").exists());
    }
}
