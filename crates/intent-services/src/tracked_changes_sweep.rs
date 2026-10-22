//! `services::tracked_changes_sweep` — prune `tracked_changes` rows stuck at a
//! working stage (intent-hq/intent#7266).
//!
//! An `unstaged`/`staged` attribution row leaves its working stage only when
//! a daemon commit or stage action picks its path up. A path git will never
//! report — git-ignored scratch, an untracked file since deleted, a file
//! reverted to `HEAD` or committed from a terminal — keeps its row forever,
//! bloating every `file-tracking.*` read and metrics recompute. This sweep
//! deletes such rows.
//!
//! A row is clean when git, asked by the repository that owns the path
//! ([`intent_git::attribution::classify_paths`], submodule-aware), reports the
//! path as [`PathState::Clean`]. A row is deleted only when **two consecutive
//! passes** (one [`SWEEP_INTERVAL`] apart) both judge it clean with the same
//! `updated_at`, so a momentary clean state never costs attribution:
//!
//! - the daemon's own commits (`agentCommit`, accept-changes) advance HEAD
//!   before moving their rows to `committed`; a pass landing in that gap
//!   only marks the row, and the stage move rescues it before the next pass;
//! - a `git stash` / branch switch / rebase window shorter than the interval
//!   leaves the path dirty again by the confirming pass.
//!
//! The delete also matches each row's exact `updated_at` and working stage,
//! so a row re-recorded or committed after it was judged survives. Rows are
//! listed before each status scan, so every listed row's file write is
//! visible to that scan. Any scan failure deletes nothing; paths git cannot
//! judge ([`PathState::Unknown`]) and rows of sandboxed (copy-on-write) agents — whose
//! files live in the sandbox, not the worktree — are kept.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use intent_core::{Result, WorkspaceId};
use intent_git::attribution::PathState;

use crate::{git_ops, Services};

/// Delay before the first sweep after startup: keeps the first scan out of
/// the daemon's startup critical path. The first pass only marks clean rows;
/// the confirming pass one [`SWEEP_INTERVAL`] later deletes them.
const SWEEP_INITIAL_DELAY: Duration = Duration::from_secs(120);

/// Interval between sweeps — also the minimum time a row must stay clean
/// before it is deleted. Locks already ignore clean paths between sweeps
/// (`agent_locks`), so this only bounds how long dead rows linger.
const SWEEP_INTERVAL: Duration = Duration::from_secs(30 * 60);

/// One row's identity for confirmation: `(id, updated_at)`.
type RowKey = (String, String);

/// Rows a pass judged clean — the confirmation input of the next pass.
pub(crate) type CleanRows = HashSet<RowKey>;

/// The result of one workspace prune.
#[derive(Debug, Default)]
pub(crate) struct PruneOutcome {
    /// Rows deleted (clean in this pass and the previous one).
    pub(crate) deleted: u64,
    /// Rows clean in this pass but not yet confirmed.
    pub(crate) clean: CleanRows,
}

impl Services {
    /// One prune pass over `workspace_id`: judge its working-stage rows,
    /// delete those that were already judged clean (unchanged) by the
    /// previous pass (`previously_clean`), and return the newly clean ones
    /// for the next pass to confirm. After deleting, recompute the workspace
    /// metrics and publish `changes:metrics-changed` (which also re-triggers
    /// the agent-locks recompute). Remote, pathless, unreadable, or
    /// non-repository workspaces delete and mark nothing.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` when a store read or delete fails.
    pub(crate) async fn prune_stale_tracked_changes(
        &self,
        workspace_id: &WorkspaceId,
        previously_clean: &CleanRows,
    ) -> Result<PruneOutcome> {
        let ws = self.store.get_workspace(workspace_id).await?;
        if ws.is_remote {
            return Ok(PruneOutcome::default());
        }
        let Some(worktree) = git_ops::worktree_path(&ws) else {
            return Ok(PruneOutcome::default());
        };
        // Missing checkout (intent-hq/intent#7183): nothing to scan, and the
        // shared presence map owns the one transition warning.
        if !self.worktree_presence.observe_workspace(&ws, &worktree) {
            return Ok(PruneOutcome::default());
        }
        let sandboxed: HashSet<String> = self
            .store
            .list_agent_session_summaries(workspace_id)
            .await?
            .into_iter()
            .filter(|s| s.sandbox_path.is_some())
            .map(|s| s.id.as_str().to_string())
            .collect();
        // Listed before the scan starts — see the module docs.
        let rows: Vec<_> = self
            .store
            .list_tracked_changes(workspace_id)
            .await?
            .into_iter()
            .filter(|r| r.stage == "unstaged" || r.stage == "staged")
            .filter(|r| r.agent_id.as_ref().is_none_or(|a| !sandboxed.contains(a)))
            .collect();
        if rows.is_empty() {
            return Ok(PruneOutcome::default());
        }
        let paths: Vec<String> = rows
            .iter()
            .map(|r| crate::file_tracking::normalize_path(&r.path))
            .collect();
        #[cfg(test)]
        if let Some(hook) = take_before_scan_hook(workspace_id) {
            hook().await;
        }
        let scan_root = worktree.clone();
        let states = match tokio::task::spawn_blocking(move || {
            intent_git::attribution::classify_paths(&scan_root, &paths)
        })
        .await
        {
            Ok(Ok(states)) => states,
            Ok(Err(e)) => {
                tracing::debug!(
                    workspace = %workspace_id.0,
                    error = %e,
                    "tracked-changes sweep: status scan failed; nothing pruned"
                );
                return Ok(PruneOutcome::default());
            }
            Err(e) => {
                tracing::warn!(
                    workspace = %workspace_id.0,
                    error = %e,
                    "tracked-changes sweep: scan task failed; nothing pruned"
                );
                return Ok(PruneOutcome::default());
            }
        };
        let (confirmed, clean): (Vec<RowKey>, Vec<RowKey>) = rows
            .into_iter()
            .zip(states)
            .filter(|(_, state)| *state == PathState::Clean)
            .map(|(r, _)| (r.id, r.updated_at))
            .partition(|key| previously_clean.contains(key));
        let mut outcome = PruneOutcome {
            deleted: 0,
            clean: clean.into_iter().collect(),
        };
        if confirmed.is_empty() {
            return Ok(outcome);
        }
        outcome.deleted = self
            .store
            .delete_unchanged_working_tracked_changes(workspace_id, &confirmed)
            .await?;
        if outcome.deleted > 0 {
            tracing::info!(
                workspace = %workspace_id.0,
                deleted = outcome.deleted,
                "tracked-changes sweep: pruned working-stage rows for paths git reports clean"
            );
            self.recompute_and_publish_metrics(workspace_id).await;
        }
        Ok(outcome)
    }

    /// One sweep over every workspace holding working-stage rows, one
    /// workspace at a time, confirming against `memory` (the previous pass's
    /// clean rows per workspace) and replacing it with this pass's. Each
    /// workspace's prune runs as a finite `store_tasks` writer, so shutdown
    /// drains an in-flight delete + metrics publication instead of aborting
    /// it midway; once shutdown has closed the writer set the pass stops. A
    /// failure in one workspace is logged and does not stop the pass.
    /// Returns the total number of rows deleted.
    pub(crate) async fn sweep_stale_tracked_changes(
        &self,
        memory: &mut HashMap<WorkspaceId, CleanRows>,
    ) -> u64 {
        let workspaces = match self
            .store
            .list_workspaces_with_working_tracked_changes()
            .await
        {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!(error = %e, "tracked-changes sweep: listing workspaces failed");
                return 0;
            }
        };
        let mut previous = std::mem::take(memory);
        let mut total = 0;
        for workspace_id in workspaces {
            let services = self.clone();
            let id = workspace_id.clone();
            let prior = previous.remove(&workspace_id).unwrap_or_default();
            let Some(prune) = self.store_tasks.spawn_draining(async move {
                services.prune_stale_tracked_changes(&id, &prior).await
            }) else {
                break;
            };
            match prune.await {
                Ok(Ok(outcome)) => {
                    total += outcome.deleted;
                    if !outcome.clean.is_empty() {
                        memory.insert(workspace_id, outcome.clean);
                    }
                }
                Ok(Err(e)) => tracing::warn!(
                    workspace = %workspace_id.0,
                    error = %e,
                    "tracked-changes sweep: workspace prune failed"
                ),
                Err(_) => break,
            }
        }
        total
    }

    /// Spawn the stale tracked-change sweep: a marking pass shortly after
    /// startup, then one pass every [`SWEEP_INTERVAL`] (each confirming and
    /// deleting what the previous one marked). Stops when shutdown closes the
    /// settings task set (like the token-usage scan) and is aborted on clean
    /// shutdown.
    #[must_use]
    pub fn spawn_tracked_changes_sweep_loop(&self) -> tokio::task::JoinHandle<()> {
        let services = self.clone();
        intent_core::spawn_daemon(async move {
            let mut memory: HashMap<WorkspaceId, CleanRows> = HashMap::new();
            let mut wait = SWEEP_INITIAL_DELAY;
            loop {
                tokio::select! {
                    biased;
                    () = services.settings_tasks.closed() => break,
                    () = tokio::time::sleep(wait) => {}
                }
                let deleted = services.sweep_stale_tracked_changes(&mut memory).await;
                if deleted > 0 {
                    tracing::info!(deleted, "tracked-changes sweep: pass complete");
                }
                wait = SWEEP_INTERVAL;
            }
        })
    }
}

/// Test seam: a one-shot future run after a workspace's rows are listed and
/// before its status scan starts, to interleave a concurrent write.
#[cfg(test)]
type BeforeScanHook = Box<dyn FnOnce() -> intent_core::BoxFuture<'static, ()> + Send>;

#[cfg(test)]
static BEFORE_SCAN_HOOKS: std::sync::Mutex<Vec<(WorkspaceId, BeforeScanHook)>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(test)]
pub(crate) fn set_before_scan_hook(workspace_id: &WorkspaceId, hook: BeforeScanHook) {
    BEFORE_SCAN_HOOKS
        .lock()
        .unwrap()
        .push((workspace_id.clone(), hook));
}

#[cfg(test)]
fn take_before_scan_hook(workspace_id: &WorkspaceId) -> Option<BeforeScanHook> {
    let mut hooks = BEFORE_SCAN_HOOKS.lock().unwrap();
    let at = hooks.iter().position(|(id, _)| id == workspace_id)?;
    Some(hooks.remove(at).1)
}

#[cfg(test)]
mod tests;
