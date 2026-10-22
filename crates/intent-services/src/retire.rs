//! `workspace.retire` (PROTOCOL §5.1): the Retired lifecycle state.
//!
//! Retiring deletes a workspace's checkout folder on disk to free space while
//! keeping every DB row (agents, conversations, notes, assets, tasks), so the
//! workspace stays viewable read-only. The flow reuses the two existing
//! teardown halves:
//!
//! - the `workspace.archive` active-work teardown (interrupt turns, cancel
//!   hooks / PR monitors / script monitors, detach guests) — run first when
//!   the workspace is still Active;
//! - the `workspace.delete` filesystem removal (worktree registration prune,
//!   rename to a trash path, parallel recursive removal) — minus the branch
//!   delete (the git branch is kept) and minus every row delete.
//!
//! Disk-backed mutations on a retired workspace are refused with
//! [`Error::WorkspaceRetired`] ([`Services::refuse_if_retired`]).
//!
//! Revive ([`Services::revive_retired_checkout`]) brings a retired workspace
//! back as a fresh blank checkout of the repository default branch, on a new
//! branch, at the same path. It is reached through `workspace.unarchive` /
//! `workspace.restore` and the turn-start auto-unarchive.

use std::path::PathBuf;

use intent_core::{AgentId, Error, Result, Workspace, WorkspaceId, WorkspaceStatus};

use crate::{
    cleanup_detached_worktree, cleanup_workspace_worktree_locked, default_workspaces_root, git_ops,
    now_iso, publish_event, workspace_dir_candidates, workspace_updated_event, Services,
};

/// What an unarchive call did (`workspace.unarchive` / `workspace.restore` /
/// the turn-start auto-unarchive).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UnarchiveOutcome {
    /// An Archived workspace flipped back to Active; its folder was intact.
    Unarchived,
    /// A Retired workspace was revived as a fresh blank checkout on `branch`;
    /// `previous_branch` (kept in the repository) is where its old work lives.
    Revived {
        previous_branch: String,
        branch: String,
    },
}

/// The fresh checkout a revive provisioned and persisted.
pub(crate) struct RevivedCheckout {
    pub(crate) previous_branch: String,
    pub(crate) branch: String,
    pub(crate) base_commit_sha: String,
}

impl Services {
    /// Refuse a call that needs the workspace's folder on disk when the
    /// workspace is retired: `Err(Error::WorkspaceRetired)`. Any other
    /// state — and a missing workspace or read failure, which the caller's
    /// own lookup reports with its usual error — passes.
    pub(crate) async fn refuse_if_retired(&self, workspace_id: &WorkspaceId) -> Result<()> {
        if workspace_id.is_chief() {
            return Ok(());
        }
        match self.store.get_workspace(workspace_id).await {
            Ok(ws) if ws.is_retired() => Err(retired_error(workspace_id)),
            _ => Ok(()),
        }
    }

    /// `workspace.retire` body (see [`intent_core::WorkspaceApi::retire_workspace`]).
    pub(crate) async fn retire_workspace_op(
        &self,
        id: WorkspaceId,
        caller_agent_id: Option<AgentId>,
    ) -> Result<Workspace> {
        self.require_workspace_manager(&id, "workspace.retire")
            .await?;
        if id.is_chief() {
            return Err(Error::InvalidParams(
                "cannot retire the chief workspace".to_string(),
            ));
        }
        let current = self.store.get_workspace(&id).await?;
        if !current.is_retired() {
            retire_supported(&current)?;
            self.require_owned_worktree(&current)?;
            // A workspace inside its delete grace window is on its way out:
            // retiring it would race the committed delete.
            if self
                .pending_workspace_deletes
                .deadline(&crate::delete_grace::PendingDeleteSubject::Workspace(
                    id.clone(),
                ))?
                .is_some()
            {
                return Err(Error::InvalidParams(format!(
                    "workspace {id} has a pending delete; cancel it before retiring"
                )));
            }
        }
        if current.is_retired() {
            // Idempotent: the folder is already gone.
            let mut ws = current;
            ws.activity = self.workspace_activity(&ws.id);
            self.attach_workspace_membership(&mut ws).await;
            return Ok(ws);
        }
        // Active → run the full archive teardown first (interrupt turns,
        // cancel hooks / PR monitors / script monitors, detach guests) and
        // its `workspace:updated` archived delta. An already-archived
        // workspace went through it when it was archived.
        if !current.archived {
            intent_core::WorkspaceApi::archive_workspace(self, id.clone(), caller_agent_id.clone())
                .await?;
        }
        // Stop every running script and refuse new launches until the folder
        // is gone, so nothing writes into the checkout being removed. After
        // the flip below, `script.start` is refused by the retired guard.
        let _scripts_closed = self.script_manager().close_workspace(&id).await;
        let retired_at = now_iso();
        let ws = {
            // Same fence as archive ↔ unarchive: the flip and its delta are
            // ordered against a concurrent unarchive.
            let _fence = self.archive_fence.acquire(&id).await;
            let flipped = self
                .store
                .retire_workspace_if_archived(&id, &retired_at)
                .await?;
            let ws = self.store.get_workspace(&id).await?;
            if !flipped && !ws.is_retired() {
                // A concurrent unarchive won the race: the workspace is live
                // again, so its folder must not be deleted.
                return Err(Error::InvalidInput(format!(
                    "workspace {id} was unarchived while it was being retired"
                )));
            }
            if flipped {
                publish_event(
                    self.event_bus.as_ref(),
                    workspace_updated_event(
                        &id,
                        &serde_json::json!({
                            "status": WorkspaceStatus::Retired,
                            "retiredAt": ws.retired_at,
                            "archived": true,
                            "archivedAt": ws.archived_at,
                        }),
                    ),
                )
                .await;
            }
            ws
        };
        // In-flight attachment uploads would otherwise keep staging chunks
        // and could commit into the deleted folder. After the flip, so a
        // `begin` racing this sweep is refused by the retired gate instead.
        self.abort_workspace_attachment_uploads(&id).await;
        // Stop the agents' provider processes (rows, transcripts and queues
        // are kept — `agent.stop` semantics): their working directory is
        // about to disappear. The initiating agent is excluded for the same
        // reason as the archive interrupt sweep: it is blocked on this call.
        if let Some(manager) = self.agent_manager() {
            let sessions = self.store.list_agent_session_summaries(&id).await?;
            let ids: Vec<AgentId> = sessions
                .into_iter()
                .map(|s| s.id)
                .filter(|a| Some(a) != caller_agent_id.as_ref())
                .collect();
            if !ids.is_empty() {
                drop(manager.stop_many(&ids).await);
            }
        }
        // Kill the workspace's terminals: their shells run inside the folder
        // being removed, and a terminal addressed only by `terminalId` would
        // otherwise keep writing into a retired workspace.
        for terminal in self.pty.list_scope(id.as_str()) {
            self.pty.kill(terminal).await;
        }
        if let Some(worktree) = git_ops::worktree_path(&ws) {
            self.git_status_cache.evict(&worktree);
        }
        self.remove_retired_checkout(&ws).await;
        let mut ws = ws;
        ws.activity = self.workspace_activity(&ws.id);
        self.attach_workspace_membership(&mut ws).await;
        Ok(ws)
    }

    /// Revive a retired workspace's checkout (PROTOCOL §5.1 "Revive"):
    /// provision a fresh linked worktree at the persisted `worktreePath` from
    /// the repository default branch on a NEW branch (the old branch, kept by
    /// retire, gets a `-N` suffix so it is never reused or touched), then flip
    /// the row Retired → Active with the new `branch` / `baseCommitSha`.
    ///
    /// The caller holds the workspace's archive fence: the retire folder
    /// sweep takes the same fence, so it can never delete the new checkout.
    /// A leftover folder at the path (a revive that won the race against the
    /// retire sweep) is detached and trashed first. Returns `None` when the
    /// row is no longer retired (nothing provisioned). On a failed row flip
    /// the new worktree and branch are removed again (best-effort).
    pub(crate) async fn revive_retired_checkout(
        &self,
        ws: &Workspace,
    ) -> Result<Option<RevivedCheckout>> {
        retire_supported(ws)?;
        let (Some(repo), Some(wt)) = (
            ws.repository_path.as_deref().map(PathBuf::from),
            ws.worktree_path.as_deref().map(PathBuf::from),
        ) else {
            return Err(retired_error(&ws.id));
        };
        let previous_branch = ws.branch.clone();
        let name = ws.id.0.clone();
        let (branch, sha, trash) = {
            let repo = repo.clone();
            let wt = wt.clone();
            let previous = previous_branch.clone();
            self.worktree_locks
                .with_lock(&repo.clone(), move || async move {
                    tokio::task::spawn_blocking(move || -> Result<_> {
                        let trash = if wt.exists() {
                            intent_git::worktree::detach_worktree(&repo, &wt)?
                        } else {
                            None
                        };
                        let base = intent_git::branches::repo_default_branch(&repo)?;
                        let desired = if previous.is_empty() {
                            "revived".to_string()
                        } else {
                            previous
                        };
                        let branch =
                            intent_git::branches::ensure_unique_branch_name(&repo, &desired)?;
                        let sha = intent_git::worktree::provision_worktree(
                            &repo,
                            &name,
                            &wt,
                            &branch,
                            Some(&base),
                            "origin",
                        )?;
                        Ok((branch, sha, trash))
                    })
                    .await
                    .map_err(|e| Error::Internal(format!("revive provisioning task failed: {e}")))?
                })
                .await?
        };
        if let Some(trash) = trash {
            let removal = async move {
                let _ =
                    tokio::task::spawn_blocking(move || cleanup_detached_worktree(&trash)).await;
            };
            drop(self.store_tasks.spawn_draining(removal));
        }
        let flipped = self
            .store
            .revive_workspace_if_retired(&ws.id, &branch, Some(&sha), &now_iso())
            .await;
        if !matches!(flipped, Ok(true)) {
            // Roll back the checkout this call provisioned: the row is not
            // (or no longer) retired, or the write failed.
            let (repo, wt, branch) = (repo.clone(), wt.clone(), branch.clone());
            self.worktree_locks
                .with_lock(&repo.clone(), move || async move {
                    let _ = tokio::task::spawn_blocking(move || {
                        let (trash, _) =
                            cleanup_workspace_worktree_locked(&repo, &wt, &branch, true);
                        if let Some(trash) = trash {
                            cleanup_detached_worktree(&trash);
                        }
                    })
                    .await;
                })
                .await;
            return flipped.map(|_| None);
        }
        // The new branch is daemon-generated: `workspace.delete` may remove
        // it (best-effort, like workspace.create's flag write).
        if let Err(e) = self
            .store
            .set_workspace_branch_auto_generated(&ws.id, true)
            .await
        {
            tracing::warn!(workspace = %ws.id.as_str(), error = %e, "revive: branch flag write failed");
        }
        self.git_status_cache.evict(&wt);
        Ok(Some(RevivedCheckout {
            previous_branch,
            branch,
            base_commit_sha: sha,
        }))
    }

    /// Ownership proof before anything is deleted (the `workspace.delete`
    /// root-anchored rule): the checkout's parent directory is named after
    /// the workspace id AND sits under a root the daemon provisions into
    /// (the boot workspaces root or the configured
    /// `workspace.worktreesLocation`). A persisted path that fails it is a
    /// folder the daemon cannot prove it created, so retire is refused with
    /// `retire-unsupported` and nothing changes. A present folder must also
    /// be a linked worktree registered in the source repository.
    fn require_owned_worktree(&self, ws: &Workspace) -> Result<()> {
        let workspaces_root = self
            .workspaces_root
            .clone()
            .unwrap_or_else(default_workspaces_root);
        let worktrees_location = self.configured_worktrees_location();
        let owned = ws.worktree_path.as_deref().is_some_and(|wt| {
            crate::checkout_parent_is_workspace_dir(wt, &ws.id)
                && (std::path::Path::new(wt).starts_with(&workspaces_root)
                    || worktrees_location
                        .as_deref()
                        .is_some_and(|loc| std::path::Path::new(wt).starts_with(loc)))
        });
        // …and the folder must be a linked worktree registered in its source
        // repository (a folder that is already gone has nothing to delete).
        let registered = owned
            && ws.worktree_path.as_deref().is_some_and(|wt| {
                let wt = std::path::Path::new(wt);
                !wt.exists()
                    || ws.repository_path.as_deref().is_some_and(|repo| {
                        intent_git::worktree::is_registered_worktree(std::path::Path::new(repo), wt)
                    })
            });
        if registered {
            Ok(())
        } else {
            Err(Error::RetireUnsupported {
                workspace_id: ws.id.as_str().to_string(),
                checkout_mode: ws.checkout_mode,
            })
        }
    }

    /// Remove a retired workspace's worktree from disk: under the per-repo
    /// worktree lock, prune the git worktree registration and rename the
    /// checkout to a trash path (so it is gone from its path when this
    /// returns), keeping the branch; then, on a detached task, recursively
    /// remove the trash and sweep the daemon-owned `<parent>/<id>/`
    /// directory (the same candidates `workspace.delete` sweeps).
    /// Best-effort: failures are logged and the workspace stays retired.
    async fn remove_retired_checkout(&self, ws: &Workspace) {
        // Held until the folder sweep below finishes: a revive takes the same
        // fence before provisioning at the same path, so the sweep can never
        // delete a revived checkout. A revive that won the race already
        // replaced the folder — nothing left to remove.
        let fence = self.archive_fence.acquire(&ws.id).await;
        if !self
            .store
            .get_workspace(&ws.id)
            .await
            .is_ok_and(|row| row.is_retired())
        {
            return;
        }
        let workspaces_root = self
            .workspaces_root
            .clone()
            .unwrap_or_else(default_workspaces_root);
        let worktrees_location = self.configured_worktrees_location();
        // [`retire_supported`] admitted only a daemon-owned git worktree
        // (`checkoutMode` worktree, not skipIsolation / remote), so both
        // paths are present.
        let worktree_path = ws.worktree_path.clone().filter(|p| !p.is_empty());
        let repo_dir = ws
            .repository_path
            .as_deref()
            .filter(|p| !p.is_empty())
            .map(PathBuf::from);
        let mut trash: Option<PathBuf> = None;
        if let (Some(repo_dir), Some(wt)) = (repo_dir, worktree_path.as_deref().map(PathBuf::from))
        {
            let branch = ws.branch.clone();
            let repo = repo_dir.clone();
            trash = self
                .worktree_locks
                .with_lock(&repo_dir, move || async move {
                    // `branch_auto_generated = false`: the branch is kept on
                    // retire (only a delete removes it).
                    let task = tokio::task::spawn_blocking(move || {
                        cleanup_workspace_worktree_locked(&repo, &wt, &branch, false)
                    })
                    .await;
                    match task {
                        Ok((trash, _)) => trash,
                        Err(e) => {
                            tracing::warn!(error = %e, "workspace.retire: worktree detach task failed");
                            None
                        }
                    }
                })
                .await;
        }
        let dirs = workspace_dir_candidates(
            &ws.id,
            worktree_path.as_deref(),
            &workspaces_root,
            worktrees_location.as_deref(),
        );
        let id = ws.id.clone();
        let removal = async move {
            let _fence = fence;
            if let Some(trash) = trash {
                let _ =
                    tokio::task::spawn_blocking(move || cleanup_detached_worktree(&trash)).await;
            }
            for dir in dirs {
                let result = tokio::task::spawn_blocking(move || {
                    intent_git::fs_remove::remove_dir_all_parallel(&dir)
                })
                .await;
                match result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => tracing::warn!(
                        workspace = %id.as_str(),
                        error = %e,
                        "workspace.retire: failed to remove workspace directory"
                    ),
                    Err(e) => tracing::warn!(
                        workspace = %id.as_str(),
                        error = %e,
                        "workspace.retire: workspace-dir removal task failed"
                    ),
                }
            }
        };
        if self.store_tasks.spawn_draining(removal).is_none() {
            tracing::warn!(
                workspace = %ws.id.as_str(),
                "workspace.retire: daemon writer admission closed; folder sweep skipped"
            );
        }
    }
}

/// Retire is only for daemon-owned git worktrees: `cow` / `direct`
/// checkouts (the folder IS the repository — deleting it loses the branch),
/// `skipIsolation` workspaces (the folder is the user's repository), remote
/// workspaces and checkout-less rows are refused with
/// [`Error::RetireUnsupported`] before anything changes.
pub(crate) fn retire_supported(ws: &Workspace) -> Result<()> {
    let refuse = |checkout_mode: Option<intent_core::CheckoutMode>| {
        Err(Error::RetireUnsupported {
            workspace_id: ws.id.as_str().to_string(),
            checkout_mode,
        })
    };
    if ws.is_remote {
        return refuse(None);
    }
    if let Some(mode @ (intent_core::CheckoutMode::Cow | intent_core::CheckoutMode::Direct)) =
        ws.checkout_mode
    {
        return refuse(Some(mode));
    }
    if ws.skip_worktree
        || ws.worktree_path.as_deref().is_none_or(str::is_empty)
        || ws.repository_path.as_deref().is_none_or(str::is_empty)
    {
        return refuse(None);
    }
    Ok(())
}

/// The [`Error::WorkspaceRetired`] refusal for `workspace_id`.
pub(crate) fn retired_error(workspace_id: &WorkspaceId) -> Error {
    Error::WorkspaceRetired {
        workspace_id: workspace_id.as_str().to_string(),
    }
}
