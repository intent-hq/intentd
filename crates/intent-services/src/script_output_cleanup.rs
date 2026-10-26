//! Deletes on-disk script output (`<data_dir>/script-output`) that is no
//! longer needed: logs older than `scripts.outputRetentionHours` (read live
//! at every sweep), compaction temp files left by a crash, and folders of workspaces that no longer exist. Script
//! removal and workspace deletion delete their own folders directly.
use std::time::Duration;

use intent_core::WorkspaceId;

use crate::script_output_store::SweepReport;
use crate::{Error, Services};

/// Time between expiry sweeps. Retention is configured in hours.
const SWEEP_INTERVAL: Duration = Duration::from_secs(3600);

impl Services {
    /// The live `scripts.outputRetentionHours` window; zero disables the
    /// expiry sweep.
    fn script_output_retention(&self) -> Duration {
        let hours = self.feature_settings_registry().map_or(
            intent_core::config::DEFAULT_SCRIPT_OUTPUT_RETENTION_HOURS,
            |r| intent_core::config::script_output_retention_hours(&r.snapshot().effective),
        );
        Duration::from_secs(u64::from(hours) * 3600)
    }

    /// Delete script run logs older than the live retention window.
    pub(crate) async fn sweep_expired_script_output(&self) -> SweepReport {
        self.sweep_script_output_older_than(self.script_output_retention())
            .await
    }

    /// Delete script run logs last written more than `max_age` ago, except
    /// the current run of a script that is still running (a service
    /// auto-restart keeps appending to that run's log). Zero deletes no logs;
    /// stale compaction temp files are deleted either way.
    pub(crate) async fn sweep_script_output_older_than(&self, max_age: Duration) -> SweepReport {
        let store = self.script_output_store();
        let mgr = self.script_manager();
        tokio::task::spawn_blocking(move || {
            store.sweep(max_age, |key| {
                mgr.run_is_live(&key.workspace, &key.script, &key.run)
            })
        })
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(error = %e, "script output sweep task failed");
            SweepReport::default()
        })
    }

    /// Delete the output folders of workspaces that no longer exist.
    /// Returns how many folders were deleted. A lookup error keeps the
    /// folder (it is retried on the next startup).
    pub(crate) async fn sweep_orphaned_script_output(&self) -> usize {
        let store = self.script_output_store();
        let ids = match tokio::task::spawn_blocking(move || store.workspace_ids()).await {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!(error = %e, "script output orphan scan failed");
                return 0;
            }
        };
        let mut removed = 0;
        for id in ids {
            let ws = WorkspaceId::from_string(id);
            if ws.is_chief() {
                continue;
            }
            match self.store().get_workspace(&ws).await {
                Err(Error::NotFound(_)) => {
                    if self.delete_workspace_script_output(&ws).await {
                        removed += 1;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(workspace = %ws, error = %e, "script output orphan check failed");
                }
            }
        }
        removed
    }

    /// Delete every on-disk log of one workspace. Best-effort: returns
    /// whether the folder is gone, logging any failure.
    pub(crate) async fn delete_workspace_script_output(&self, ws: &WorkspaceId) -> bool {
        let store = self.script_output_store();
        let id = ws.as_str().to_owned();
        match tokio::task::spawn_blocking(move || store.delete_workspace(&id)).await {
            Ok(Ok(())) => true,
            Ok(Err(e)) => {
                tracing::warn!(workspace = %ws, error = %e, "script output delete failed");
                false
            }
            Err(e) => {
                tracing::warn!(workspace = %ws, error = %e, "script output delete task failed");
                false
            }
        }
    }

    /// Spawn the script output cleanup loop: at startup it deletes folders of
    /// workspaces that no longer exist, then sweeps expired logs right away
    /// and every hour after. Stops when the settings task group closes; the
    /// composition root also aborts it on shutdown.
    #[must_use]
    pub fn spawn_script_output_sweep_loop(&self) -> tokio::task::JoinHandle<()> {
        let services = self.clone();
        intent_core::spawn_daemon(async move {
            let orphans = services.sweep_orphaned_script_output().await;
            if orphans > 0 {
                tracing::info!(
                    orphans,
                    "script output: deleted folders of deleted workspaces"
                );
            }
            loop {
                let report = services.sweep_expired_script_output().await;
                if report.deleted > 0 || report.failed > 0 || report.stale_temps_deleted > 0 {
                    tracing::info!(
                        deleted = report.deleted,
                        skipped_live = report.skipped_live,
                        failed = report.failed,
                        stale_temps_deleted = report.stale_temps_deleted,
                        "script output: expiry sweep complete"
                    );
                }
                tokio::select! {
                    biased;
                    () = services.feature_settings_tasks().closed() => break,
                    () = tokio::time::sleep(SWEEP_INTERVAL) => {}
                }
            }
        })
    }
}
