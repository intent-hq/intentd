//! Startup sweep that finishes deletes a previous run left half-done.
//!
//! A Postgres workspace or agent delete purges in bounded batches and keeps a
//! `delete_purge` record until its final transaction; one stopped part-way (a
//! crash, a failed batch) leaves the workspace or agent visible with part of
//! its children gone. The store does not finish it on its own, since that
//! would skip the service teardown. This sweep re-issues the full service
//! delete for each record ([`Store::pending_deletes`]), so the deleted
//! events, worktree and script cleanup, home stops and agent release all run.
//! `SQLite` keeps no record, so on `SQLite` the sweep finds nothing.
//!
//! The daemon awaits the sweep to completion before any startup restore, so
//! the restores only ever see finished deletes: a deleted workspace's rows
//! are gone, and a surviving parent's completion watch on a deleted child is
//! settled by the normal startup reconciliation (the child is missing). A
//! delete that fails keeps its record for the next start; its workspace or
//! agent is then live, and the restores load it like any other.
//!
//! A failed workspace delete may already have done part of its teardown
//! before it failed: its agents stopped and `agent:deleted` published, the
//! completion watches on them settled and their rows dropped. The restores
//! then load the workspace as it is. The delete's own detached cleanup
//! (worktree and file removal, spawned with the store's draining tasks)
//! still runs in the background after the sweep returns, as it does for any
//! `workspace.delete`.
//!
//! A daemon stop ends the sweep between records: the delete in progress
//! finishes, and the rest keep their records for the next start.
//!
//! The trade-off: startup blocks until the pending deletes finish. That only
//! happens after a crash (or a failed batch) in the middle of a delete, on
//! Postgres, and the remaining work is what that delete left.
//!
//! [`Store::pending_deletes`]: intent_store::Store::pending_deletes

use std::collections::HashSet;
use std::time::Duration;

use intent_core::{Caller, Error, Result, WorkspaceId};
use intent_store::PendingDelete;

use crate::{Services, WorkspaceApi};

/// A delete still running after this long is logged, and again each time.
const SLOW_DELETE_LOG_EVERY: Duration = Duration::from_secs(5);

/// What one [`Services::finish_pending_deletes`] run did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PendingDeleteSweep {
    /// Deletes re-issued and finished (a record whose subject was already
    /// gone counts as finished).
    pub finished: usize,
    /// Deletes that failed; each was logged and keeps its record for the
    /// next start or an explicit delete.
    pub failed: usize,
    /// Agent records already covered by their workspace's finished delete.
    pub covered: usize,
    /// Records not started because the daemon began stopping; they keep
    /// their records for the next start.
    pub left_for_next_start: usize,
}

impl Services {
    /// Finish every delete the store lists as pending, one at a time,
    /// through the same service paths as `workspace.delete` and
    /// `agent.delete`, with daemon authority. Returns when every record has
    /// been handled.
    ///
    /// Workspace records go first; an agent record whose workspace delete
    /// finished here is covered by it. A failure is logged and the sweep
    /// goes on. `stopping` is checked before each record: once it reports
    /// true, the sweep returns and leaves the rest for the next start.
    ///
    /// # Errors
    ///
    /// Only if listing the pending records fails.
    pub async fn finish_pending_deletes(
        &self,
        stopping: impl Fn() -> bool,
    ) -> Result<PendingDeleteSweep> {
        let records = self.store.pending_deletes().await?;
        let mut sweep = PendingDeleteSweep::default();
        if records.is_empty() {
            return Ok(sweep);
        }
        let total = records.len();
        tracing::info!(
            total,
            "finishing deletes left pending by a previous run before restoring state"
        );
        let (workspaces, agents): (Vec<_>, Vec<_>) = records
            .into_iter()
            .partition(|record| matches!(record, PendingDelete::Workspace(_)));
        let mut deleted_workspaces: HashSet<WorkspaceId> = HashSet::new();
        for (index, record) in workspaces.into_iter().chain(agents).enumerate() {
            if stopping() {
                sweep.left_for_next_start = total - index;
                tracing::info!(
                    left = sweep.left_for_next_start,
                    "daemon stopping: pending deletes left for the next start"
                );
                break;
            }
            if let PendingDelete::Agent { workspace_id, .. } = &record {
                if deleted_workspaces.contains(workspace_id) {
                    sweep.covered += 1;
                    continue;
                }
            }
            match self.finish_pending_delete(&record).await {
                Ok(()) => {
                    sweep.finished += 1;
                    if let PendingDelete::Workspace(id) = record {
                        deleted_workspaces.insert(id);
                    }
                }
                Err(Error::NotFound(_)) => {
                    tracing::info!(record = ?record, "pending delete: already gone");
                    sweep.finished += 1;
                }
                Err(error) => {
                    tracing::warn!(record = ?record, %error, "pending delete failed; its record stays for the next start");
                    sweep.failed += 1;
                }
            }
            tracing::info!(done = index + 1, total, "pending delete sweep progress");
        }
        tracing::info!(?sweep, "pending delete sweep done");
        Ok(sweep)
    }

    /// Re-issue one delete, logging while it runs long.
    async fn finish_pending_delete(&self, record: &PendingDelete) -> Result<()> {
        let started = tokio::time::Instant::now();
        let delete = intent_core::with_caller(Caller::Daemon, async {
            match record.clone() {
                PendingDelete::Workspace(id) => self.delete_workspace(id).await,
                PendingDelete::Agent {
                    workspace_id,
                    agent_id,
                } => self
                    .agent_delete(agent_id, Some(workspace_id))
                    .await
                    .map(|_| ()),
            }
        });
        tokio::pin!(delete);
        let mut slow =
            tokio::time::interval_at(started + SLOW_DELETE_LOG_EVERY, SLOW_DELETE_LOG_EVERY);
        let outcome = loop {
            tokio::select! {
                outcome = &mut delete => break outcome,
                _ = slow.tick() => tracing::info!(
                    record = ?record,
                    elapsed_secs = started.elapsed().as_secs(),
                    "pending delete still running"
                ),
            }
        };
        if started.elapsed() >= SLOW_DELETE_LOG_EVERY {
            tracing::info!(
                record = ?record,
                elapsed_secs = started.elapsed().as_secs(),
                "pending delete took a while"
            );
        }
        outcome
    }
}
