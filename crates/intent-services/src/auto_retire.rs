//! Automatic purge (PROTOCOL §5.1 "Automatic purge", §5.12
//! `workspace.purgeArchivedAfterDays`, formerly
//! `workspace.autoRetireArchivedAfterDays`): a periodic sweep that retires
//! (purges the folder of) every workspace archived longer than the
//! configured number of days, through the same path and `workspace:updated`
//! delta as `workspace.retire`.
//!
//! The setting is read live at every sweep (absent = off). Remote,
//! guest-shared and retire-unsupported workspaces are skipped, and so is a
//! workspace with a pending grace-window delete, and one whose folder holds
//! work the purge would destroy
//! ([`crate::local_work::LocalWorkRule::Purge`]): uncommitted changes in
//! any root, unpushed commits in a secondary root inside the folder (unless
//! a merged PR covers that root's `HEAD`), or a root that could not be
//! evaluated. Each workspace is retired
//! independently: a failure is logged and counted, never aborts the sweep.
//! Retired rows are not archived-and-due any more, so a sweep is idempotent.

use std::time::Duration;

use intent_core::{Workspace, WorkspaceListFilter};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::Services;

/// Time between sweeps. The window is configured in days.
const SWEEP_INTERVAL: Duration = Duration::from_secs(3600);

/// Delay before the first sweep, so it stays clear of the startup burst.
const FIRST_SWEEP_DELAY: Duration = Duration::from_secs(120);

/// What one auto-retire sweep did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AutoRetireReport {
    /// Workspaces retired by this sweep.
    pub retired: usize,
    /// Overdue-looking archived workspaces skipped (remote, guest-shared,
    /// retire-unsupported — including a refused retire attempt — pending
    /// delete, uncommitted or unpushed local work, or no readable
    /// `archivedAt`).
    pub skipped: usize,
    /// Workspaces whose retire attempt failed with any other error (logged
    /// as a warning; retried next sweep).
    pub failed: usize,
}

/// How one retire attempt counts in an [`AutoRetireReport`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Attempt {
    Retired,
    /// `retire-unsupported`: the workspace can never be retired as it is.
    Skipped,
    /// Any other error; retried on the next sweep.
    Failed,
}

/// `Ok(None)` is the automatic op declining because the workspace is no
/// longer due (unarchived, re-archived or retired since the sweep listed it).
fn classify<T>(result: &intent_core::Result<Option<T>>) -> Attempt {
    match result {
        Ok(Some(_)) => Attempt::Retired,
        Ok(None) | Err(intent_core::Error::RetireUnsupported { .. }) => Attempt::Skipped,
        Err(_) => Attempt::Failed,
    }
}

impl Services {
    /// One sweep at `now`, reading `workspace.purgeArchivedAfterDays` (or
    /// its legacy predecessor) from the live settings snapshot. A no-op when
    /// the setting is off.
    pub(crate) async fn sweep_auto_retire_at(&self, now: OffsetDateTime) -> AutoRetireReport {
        let days = crate::settings::purge_archived_after_days(&self.effective_settings());
        self.sweep_auto_retire_after(days, now).await
    }

    /// One sweep at `now` with an explicit window: retire every archived,
    /// not yet retired workspace whose `archivedAt` is older than
    /// `now − days`. `None` does nothing.
    pub(crate) async fn sweep_auto_retire_after(
        &self,
        days: Option<u32>,
        now: OffsetDateTime,
    ) -> AutoRetireReport {
        let mut report = AutoRetireReport::default();
        let Some(days) = days else {
            return report;
        };
        // A window reaching before the earliest representable date (e.g.
        // u32::MAX days) can never be due: skip the sweep instead of
        // overflowing the subtraction.
        let Some(cutoff) = now.checked_sub(time::Duration::days(i64::from(days))) else {
            return report;
        };
        let workspaces = match self
            .store
            .list_workspaces(WorkspaceListFilter::WITH_ARCHIVED)
            .await
        {
            Ok(list) => list,
            Err(e) => {
                tracing::warn!(error = %e, "auto-retire: listing workspaces failed");
                return report;
            }
        };
        for ws in workspaces {
            if !ws.archived || ws.is_retired() || ws.id.is_chief() {
                continue;
            }
            let dated = ws
                .archived_at
                .as_deref()
                .is_some_and(|stamp| OffsetDateTime::parse(stamp, &Rfc3339).is_ok());
            if !dated {
                report.skipped += 1;
                continue;
            }
            if !crate::retire::auto_retire_due(&ws, cutoff) {
                continue;
            }
            if !self.auto_retire_eligible(&ws).await {
                report.skipped += 1;
                continue;
            }
            if let Some(reason) = self
                .blocked_by_local_work(&ws, crate::local_work::LocalWorkRule::Purge)
                .await
            {
                tracing::debug!(workspace = %ws.id, reason, "auto-retire: local work in the folder, skipped");
                report.skipped += 1;
                continue;
            }
            // The listed row may be stale: the automatic op re-checks
            // "still archived before the cutoff" under the archive fence and
            // never archives an Active workspace.
            let result = self.auto_retire_workspace_op(ws.id.clone(), cutoff).await;
            match classify(&result) {
                Attempt::Retired => {
                    report.retired += 1;
                    tracing::info!(
                        workspace = %ws.id,
                        archived_at = ws.archived_at.as_deref().unwrap_or_default(),
                        retire_after_days = days,
                        "auto-retire: retired archived workspace"
                    );
                }
                // A refusal the workspace will keep returning every sweep
                // (e.g. a folder the daemon does not own): expected, so it is
                // skipped quietly instead of warning every hour.
                Attempt::Skipped => {
                    report.skipped += 1;
                    match &result {
                        Err(e) => {
                            tracing::debug!(workspace = %ws.id, error = %e, "auto-retire: retire unsupported, skipped");
                        }
                        Ok(_) => {
                            tracing::debug!(workspace = %ws.id, "auto-retire: no longer due, skipped");
                        }
                    }
                }
                Attempt::Failed => {
                    report.failed += 1;
                    if let Err(e) = &result {
                        tracing::warn!(workspace = %ws.id, error = %e, "auto-retire: retire failed");
                    }
                }
            }
        }
        report
    }

    /// Whether an overdue archived workspace may be retired automatically:
    /// not remote / retire-unsupported, not shared with guests, and not
    /// inside a pending delete's grace window. A failed lookup is treated as
    /// not eligible (retried next sweep).
    async fn auto_retire_eligible(&self, ws: &Workspace) -> bool {
        if crate::retire::retire_supported(ws).is_err() {
            return false;
        }
        match self.store.count_workspace_guests(&ws.id).await {
            Ok(guests) if guests.collaborators == 0 && guests.open_invites == 0 => {}
            Ok(_) => return false,
            Err(e) => {
                tracing::warn!(workspace = %ws.id, error = %e, "auto-retire: guest lookup failed");
                return false;
            }
        }
        matches!(
            self.pending_workspace_deletes.deadline(
                &crate::delete_grace::PendingDeleteSubject::Workspace(ws.id.clone(),)
            ),
            Ok(None)
        )
    }

    /// Spawn the auto-retire loop: a first sweep shortly after startup, then
    /// one every hour, each reading the setting live (so turning it on from
    /// Settings applies within the hour, without a restart). Stops when the
    /// settings task group closes; the composition root also aborts it on
    /// shutdown.
    #[must_use]
    pub fn spawn_auto_retire_loop(&self) -> tokio::task::JoinHandle<()> {
        let services = self.clone();
        intent_core::spawn_daemon(async move {
            let mut delay = FIRST_SWEEP_DELAY;
            loop {
                tokio::select! {
                    biased;
                    () = services.settings_tasks.closed() => break,
                    () = tokio::time::sleep(delay) => {}
                }
                delay = SWEEP_INTERVAL;
                let report = services
                    .sweep_auto_retire_at(OffsetDateTime::now_utc())
                    .await;
                if report != AutoRetireReport::default() {
                    tracing::info!(
                        retired = report.retired,
                        skipped = report.skipped,
                        failed = report.failed,
                        "auto-retire: sweep complete"
                    );
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{classify, Attempt};
    use intent_core::Error;

    /// A retire-unsupported refusal is skipped (debug only, it would repeat
    /// every sweep); any other error is a failure; success is a retire.
    #[test]
    fn classify_separates_unsupported_refusals_from_failures() {
        assert_eq!(classify(&Ok::<_, Error>(Some(()))), Attempt::Retired);
        assert_eq!(classify(&Ok::<Option<()>, Error>(None)), Attempt::Skipped);
        let unsupported: intent_core::Result<Option<()>> = Err(Error::RetireUnsupported {
            workspace_id: "ws-1".to_string(),
            checkout_mode: None,
        });
        assert_eq!(classify(&unsupported), Attempt::Skipped);
        for err in [
            Error::Internal("disk".to_string()),
            Error::InvalidInput("unarchived while retiring".to_string()),
            Error::InvalidParams("pending delete".to_string()),
        ] {
            assert_eq!(classify(&Err::<Option<()>, _>(err)), Attempt::Failed);
        }
    }
}
