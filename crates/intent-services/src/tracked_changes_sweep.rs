//! `services::tracked_changes_sweep` — prune `tracked_changes` rows stuck at a
//! working stage (intent-hq/intent#7266).
//!
//! An `unstaged`/`staged` attribution row leaves its working stage only when
//! a daemon commit or stage action picks its path up. A path git will never
//! report — git-ignored scratch, an untracked file since deleted, a file
//! reverted to `HEAD` or committed from a terminal — keeps its row forever:
//! the row bloats every `file-tracking.*` read and metrics recompute and
//! locks its path (`agent_locks`). This sweep deletes such rows.
//!
//! A row is stale when git, asked by the repository that owns the path
//! ([`intent_git::attribution::classify_paths`], submodule-aware), reports the
//! path as [`PathState::Clean`]. Safety against a concurrent agent write:
//! the rows are listed **before** the status scan starts, so every listed
//! row's file write (which precedes its recording) is visible to the scan;
//! and the delete matches each row's exact `updated_at`, so a row re-recorded
//! after the listing survives. Any scan failure deletes nothing, and paths
//! whose owning repository cannot be read ([`PathState::Unknown`]) are kept.

use std::time::Duration;

use intent_core::{Result, WorkspaceId};
use intent_git::attribution::PathState;

use crate::{changes_metrics_changed_event, git_ops, publish_event, Services};

/// Delay before the first sweep after startup: long enough to stay out of
/// the daemon's startup critical path, short enough to clear the backlog
/// early in a session.
const SWEEP_INITIAL_DELAY: Duration = Duration::from_secs(120);

/// Interval between sweeps. Locks already ignore clean paths between sweeps
/// (`agent_locks`), so this only bounds how long dead rows linger.
const SWEEP_INTERVAL: Duration = Duration::from_secs(30 * 60);

impl Services {
    /// Delete `workspace_id`'s working-stage tracked-change rows whose path
    /// git reports as clean, then recompute the workspace metrics and publish
    /// `changes:metrics-changed` (which also re-triggers the agent-locks
    /// recompute). Returns the number of rows deleted; remote, pathless,
    /// unreadable, or non-repository workspaces delete nothing.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` when a store read or delete fails.
    pub(crate) async fn prune_stale_tracked_changes(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<u64> {
        let ws = self.store.get_workspace(workspace_id).await?;
        if ws.is_remote {
            return Ok(0);
        }
        let Some(worktree) = git_ops::worktree_path(&ws) else {
            return Ok(0);
        };
        // Listed before the scan starts — see the module docs.
        let rows: Vec<_> = self
            .store
            .list_tracked_changes(workspace_id)
            .await?
            .into_iter()
            .filter(|r| r.stage == "unstaged" || r.stage == "staged")
            .collect();
        if rows.is_empty() {
            return Ok(0);
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
                return Ok(0);
            }
            Err(e) => {
                tracing::warn!(
                    workspace = %workspace_id.0,
                    error = %e,
                    "tracked-changes sweep: scan task failed; nothing pruned"
                );
                return Ok(0);
            }
        };
        let stale: Vec<(String, String)> = rows
            .into_iter()
            .zip(states)
            .filter(|(_, state)| *state == PathState::Clean)
            .map(|(r, _)| (r.id, r.updated_at))
            .collect();
        if stale.is_empty() {
            return Ok(0);
        }
        let deleted = self
            .store
            .delete_unchanged_working_tracked_changes(workspace_id, &stale)
            .await?;
        if deleted > 0 {
            tracing::info!(
                workspace = %workspace_id.0,
                deleted,
                "tracked-changes sweep: pruned working-stage rows for paths git reports clean"
            );
            self.publish_metrics_after_prune(workspace_id).await;
        }
        Ok(deleted)
    }

    /// Recompute the workspace's line-change aggregates from the pruned
    /// table and broadcast them, so clients and the agent-locks loop see the
    /// shrunken state. Best-effort.
    async fn publish_metrics_after_prune(&self, workspace_id: &WorkspaceId) {
        if let Err(e) = crate::metrics::recompute(&self.store, workspace_id).await {
            tracing::warn!(error = %e, "tracked-changes sweep: metrics recompute failed");
        }
        let metrics = match self.store.get_workspace_metrics(workspace_id).await {
            Ok(Some(ws)) => {
                let agents = self
                    .store
                    .list_agent_metrics_for_workspace(workspace_id)
                    .await
                    .unwrap_or_default();
                crate::metrics::workspace_metrics_value(&ws, &agents)
            }
            _ => serde_json::Value::Null,
        };
        publish_event(
            self.event_bus.as_ref(),
            changes_metrics_changed_event(workspace_id, &metrics),
        )
        .await;
    }

    /// One sweep over every workspace holding working-stage rows, one
    /// workspace at a time. Each workspace's prune runs as a finite
    /// `store_tasks` writer, so shutdown drains an in-flight delete + metrics
    /// publication instead of aborting it midway; once shutdown has closed
    /// the writer set the pass stops. A failure in one workspace is logged and
    /// does not stop the pass. Returns the total number of rows deleted.
    pub(crate) async fn sweep_stale_tracked_changes(&self) -> u64 {
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
        let mut total = 0;
        for workspace_id in workspaces {
            let services = self.clone();
            let id = workspace_id.clone();
            let Some(prune) = self
                .store_tasks
                .spawn_draining(async move { services.prune_stale_tracked_changes(&id).await })
            else {
                break;
            };
            match prune.await {
                Ok(Ok(n)) => total += n,
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

    /// Spawn the stale tracked-change sweep: one pass shortly after startup
    /// (clearing any backlog), then one every [`SWEEP_INTERVAL`]. Stops when
    /// shutdown closes the settings task set (like the token-usage scan) and
    /// is aborted on clean shutdown.
    #[must_use]
    pub fn spawn_tracked_changes_sweep_loop(&self) -> tokio::task::JoinHandle<()> {
        let services = self.clone();
        intent_core::spawn_daemon(async move {
            let mut wait = SWEEP_INITIAL_DELAY;
            loop {
                tokio::select! {
                    biased;
                    () = services.settings_tasks.closed() => break,
                    () = tokio::time::sleep(wait) => {}
                }
                let deleted = services.sweep_stale_tracked_changes().await;
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
