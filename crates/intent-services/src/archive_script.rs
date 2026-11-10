//! `archiveScript` from `.intent/config.json` (intent-hq/intent#8850).
//!
//! The script runs once, when a workspace leaves Active: on archive (manual
//! or automatic), on delete of an Active workspace, and on purge (retire) of
//! an Active workspace. Deleting or purging an already archived workspace
//! does not run it again, but waits for a run that is still going so the
//! folder outlives it. Unarchive also waits for it, so work never resumes
//! while the cleanup is still running. A run is claimed only under the
//! workspace's archive fence (archive tail, retire) or after the row is gone
//! (delete), so "fence held and gate free" means no run starts until the
//! fence is released. A failed run never blocks the lifecycle step; for
//! archive and purge it is shown on the workspace card (`statusMessage`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use intent_core::{Workspace, WorkspaceId, WorkspaceUpdate};
use tokio::sync::OwnedMutexGuard;

use crate::feature_ports::{ArchiveScriptOutcome, ArchiveScriptRun, ArchiveScriptTrigger};
use crate::{git_ops, repo_config, Services};

/// One async gate per workspace, held for the whole run of its script.
#[derive(Clone, Default)]
pub(crate) struct ArchiveScriptGates(Arc<Mutex<HashMap<WorkspaceId, Arc<tokio::sync::Mutex<()>>>>>);

impl ArchiveScriptGates {
    fn gate(&self, id: &WorkspaceId) -> Arc<tokio::sync::Mutex<()>> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(id.clone())
            .or_default()
            .clone()
    }

    fn forget(&self, id: &WorkspaceId) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(id);
    }
}

/// A resolved run with its workspace's gate: held when it was free, else
/// taken once the run still in progress ends.
pub(crate) struct ClaimedArchiveScript {
    run: ArchiveScriptRun,
    gate: GateClaim,
}

enum GateClaim {
    Held(OwnedMutexGuard<()>),
    Busy(Arc<tokio::sync::Mutex<()>>),
}

impl Services {
    /// Resolve `ws`'s archive script and take its gate. `None` when there is
    /// nothing to run: no script configured, a plain folder, a skip-worktree
    /// or remote row, or a folder that no longer exists.
    pub(crate) async fn claim_archive_script(
        &self,
        ws: &Workspace,
        trigger: ArchiveScriptTrigger,
    ) -> Option<ClaimedArchiveScript> {
        if ws.plain_folder || ws.skip_worktree || ws.is_remote {
            return None;
        }
        let worktree = git_ops::worktree_path(ws)?;
        if !tokio::fs::metadata(&worktree)
            .await
            .is_ok_and(|meta| meta.is_dir())
        {
            return None;
        }
        let script = repo_config::read_repo_config(&worktree)
            .await
            .archive_script
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())?;
        // Never waits here: callers may hold the archive fence.
        let gate = self.archive_script_gates.gate(&ws.id);
        let gate = match gate.clone().try_lock_owned() {
            Ok(guard) => GateClaim::Held(guard),
            Err(_) => GateClaim::Busy(gate),
        };
        Some(ClaimedArchiveScript {
            run: ArchiveScriptRun {
                workspace_id: ws.id.clone(),
                trigger,
                worktree,
                script,
                repo_path: ws.repository_path.clone().unwrap_or_default(),
                branch_name: ws.branch.clone(),
                source_branch: ws.base_ref.clone().unwrap_or_default(),
            },
            gate,
        })
    }

    /// Run a claimed script and release its gate. With `report_on_card`, a
    /// failure becomes the workspace's `statusMessage`.
    pub(crate) async fn run_claimed_archive_script(
        &self,
        claimed: ClaimedArchiveScript,
        report_on_card: bool,
    ) -> ArchiveScriptOutcome {
        let ClaimedArchiveScript { run, gate } = claimed;
        let gate = match gate {
            GateClaim::Held(guard) => guard,
            GateClaim::Busy(gate) => gate.lock_owned().await,
        };
        let workspace_id = run.workspace_id.clone();
        let trigger = run.trigger;
        let outcome = self.ports.runtime().run_archive_script(self, run).await;
        drop(gate);
        let ran = matches!(
            outcome,
            ArchiveScriptOutcome::Succeeded
                | ArchiveScriptOutcome::Failed { .. }
                | ArchiveScriptOutcome::TimedOut
        );
        if ran && trigger != ArchiveScriptTrigger::Delete {
            self.leave_archive_script_notice(&workspace_id, outcome)
                .await;
        }
        if report_on_card {
            if let Some(message) = outcome.failure_message() {
                let update = WorkspaceUpdate {
                    status_message: Some(message),
                    ..WorkspaceUpdate::default()
                };
                let write = intent_core::with_caller(
                    intent_core::Caller::Daemon,
                    intent_core::WorkspaceApi::update_workspace(self, workspace_id.clone(), update),
                )
                .await;
                if let Err(error) = write {
                    tracing::warn!(
                        workspace = %workspace_id.as_str(),
                        error = %error,
                        "archive script failure could not be shown on the workspace card"
                    );
                }
            }
        }
        outcome
    }

    /// Leave the one-time "archive script ran" prompt notice on every agent
    /// of the workspace (best-effort).
    pub(crate) async fn leave_archive_script_notice(
        &self,
        workspace_id: &WorkspaceId,
        outcome: ArchiveScriptOutcome,
    ) {
        let failure = outcome.failure_message();
        let notice = crate::agent_manager::archive_script_prompt_notice(failure.as_deref());
        let sessions = match self.store.list_agent_session_summaries(workspace_id).await {
            Ok(sessions) => sessions,
            Err(error) => {
                tracing::warn!(
                    workspace = %workspace_id.as_str(),
                    error = %error,
                    "archive script notice: agent list failed"
                );
                return;
            }
        };
        for session in sessions {
            if let Err(error) = self
                .store
                .set_agent_session_metadata_key(
                    workspace_id,
                    &session.id,
                    intent_core::PENDING_ARCHIVE_SCRIPT_NOTICE_KEY,
                    &notice,
                    None,
                    None,
                )
                .await
            {
                tracing::warn!(
                    workspace = %workspace_id.as_str(),
                    agent = %session.id,
                    error = %error,
                    "archive script notice could not be left for an agent"
                );
            }
        }
    }

    /// Wait until no archive script is running for `id`.
    pub(crate) async fn wait_for_archive_script(&self, id: &WorkspaceId) {
        drop(self.archive_script_gates.gate(id).lock_owned().await);
    }

    /// The archive fence for an unarchive: taken only once no archive script
    /// runs, so agents never resume while the cleanup is still going. Waits
    /// without the fence; a run claimed meanwhile (under the fence) is
    /// waited for in turn.
    pub(crate) async fn archive_fence_after_archive_script(
        &self,
        id: &WorkspaceId,
    ) -> crate::ArchiveFenceGuard {
        loop {
            self.wait_for_archive_script(id).await;
            let fence = self.archive_fence.acquire(id).await;
            if self.archive_script_gates.gate(id).try_lock().is_ok() {
                return fence;
            }
            drop(fence);
        }
    }

    /// Drop `id`'s gate once the workspace is deleted.
    pub(crate) fn forget_archive_script_gate(&self, id: &WorkspaceId) {
        self.archive_script_gates.forget(id);
    }
}
