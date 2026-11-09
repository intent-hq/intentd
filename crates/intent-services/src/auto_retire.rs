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
//! work the purge would destroy. For a linked worktree that is
//! [`crate::local_work::LocalWorkRule::Purge`]: uncommitted changes in any
//! root, unpushed commits in a secondary root inside the folder (unless a
//! merged PR covers that root's `HEAD`), or a root that could not be
//! evaluated. A standalone `cow` / `direct` checkout is its own repository,
//! so its branches exist nowhere else: it takes the stricter
//! [`crate::local_work::LocalWorkRule::PurgeStandalone`] deep scan of every
//! repository in the folder. Each workspace is retired independently: a
//! failure is logged and counted, never aborts the sweep. Retired rows are
//! not archived-and-due any more, so a sweep is idempotent. The sweep
//! summary counts skips per reason, so a setting that cannot act on most
//! workspaces is visible in the log.

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
    /// Overdue-looking archived workspaces skipped, for any reason: the sum
    /// of the `skipped_*` counts.
    pub skipped: usize,
    /// Skipped as retire-unsupported (remote, skipIsolation, plain folder,
    /// a folder the daemon cannot prove it owns), including a refused
    /// retire attempt.
    pub skipped_unsupported: usize,
    /// Skipped because the workspace is shared with guests (or the guest
    /// lookup failed).
    pub skipped_shared: usize,
    /// Skipped inside a pending delete's grace window.
    pub skipped_pending_delete: usize,
    /// Skipped because the folder holds work the purge would destroy (or
    /// could not be evaluated).
    pub skipped_local_work: usize,
    /// Skipped for an unreadable `archivedAt`.
    pub skipped_undated: usize,
    /// Skipped because the workspace stopped being due while it was being
    /// retired (unarchived, or new work appeared under the fence).
    pub skipped_no_longer_due: usize,
    /// Workspaces whose retire attempt failed with any other error (logged
    /// as a warning; retried next sweep).
    pub failed: usize,
}

/// Why the sweep skipped a workspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Skip {
    Unsupported,
    Shared,
    PendingDelete,
    LocalWork,
    Undated,
    NoLongerDue,
}

impl AutoRetireReport {
    fn skip(&mut self, reason: Skip) {
        self.skipped += 1;
        let count = match reason {
            Skip::Unsupported => &mut self.skipped_unsupported,
            Skip::Shared => &mut self.skipped_shared,
            Skip::PendingDelete => &mut self.skipped_pending_delete,
            Skip::LocalWork => &mut self.skipped_local_work,
            Skip::Undated => &mut self.skipped_undated,
            Skip::NoLongerDue => &mut self.skipped_no_longer_due,
        };
        *count += 1;
    }
}

/// How one retire attempt counts in an [`AutoRetireReport`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Attempt {
    Retired,
    /// Skipped for good (`retire-unsupported`) or until it is due again.
    Skipped(Skip),
    /// Any other error; retried on the next sweep.
    Failed,
}

/// `Ok(None)` is the automatic op declining because the workspace is no
/// longer due (unarchived, re-archived or retired since the sweep listed it,
/// or a standalone checkout that gained work under the fence).
fn classify<T>(result: &intent_core::Result<Option<T>>) -> Attempt {
    match result {
        Ok(Some(_)) => Attempt::Retired,
        Ok(None) => Attempt::Skipped(Skip::NoLongerDue),
        Err(intent_core::Error::RetireUnsupported { .. }) => Attempt::Skipped(Skip::Unsupported),
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
                report.skip(Skip::Undated);
                continue;
            }
            if !crate::retire::auto_retire_due(&ws, cutoff) {
                continue;
            }
            if let Some(reason) = self.auto_retire_ineligible(&ws).await {
                report.skip(reason);
                continue;
            }
            let rule = if crate::retire::is_standalone_checkout(&ws) {
                crate::local_work::LocalWorkRule::PurgeStandalone
            } else {
                crate::local_work::LocalWorkRule::Purge
            };
            if let Some(reason) = self.blocked_by_local_work(&ws, rule).await {
                tracing::debug!(workspace = %ws.id, reason, "auto-retire: local work in the folder, skipped");
                report.skip(Skip::LocalWork);
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
                Attempt::Skipped(reason) => {
                    report.skip(reason);
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

    /// Why an overdue archived workspace may not be retired automatically:
    /// remote / retire-unsupported or a folder the daemon cannot prove it
    /// owns, shared with guests, or inside a pending delete's grace window.
    /// `None` = eligible. A failed lookup is treated as not eligible
    /// (retried next sweep).
    async fn auto_retire_ineligible(&self, ws: &Workspace) -> Option<Skip> {
        if crate::retire::retire_supported(ws).is_err() || self.require_owned_worktree(ws).is_err()
        {
            return Some(Skip::Unsupported);
        }
        match self.store.count_workspace_guests(&ws.id).await {
            Ok(guests) if guests.collaborators == 0 && guests.open_invites == 0 => {}
            Ok(_) => return Some(Skip::Shared),
            Err(e) => {
                tracing::warn!(workspace = %ws.id, error = %e, "auto-retire: guest lookup failed");
                return Some(Skip::Shared);
            }
        }
        match self.pending_workspace_deletes.deadline(
            &crate::delete_grace::PendingDeleteSubject::Workspace(ws.id.clone()),
        ) {
            Ok(None) => None,
            _ => Some(Skip::PendingDelete),
        }
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
                        skipped_unsupported = report.skipped_unsupported,
                        skipped_local_work = report.skipped_local_work,
                        skipped_shared = report.skipped_shared,
                        skipped_pending_delete = report.skipped_pending_delete,
                        skipped_undated = report.skipped_undated,
                        skipped_no_longer_due = report.skipped_no_longer_due,
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
    use super::{classify, Attempt, Skip};
    use intent_core::Error;

    /// A retire-unsupported refusal is skipped (debug only, it would repeat
    /// every sweep); any other error is a failure; success is a retire.
    #[test]
    fn classify_separates_unsupported_refusals_from_failures() {
        assert_eq!(classify(&Ok::<_, Error>(Some(()))), Attempt::Retired);
        assert_eq!(
            classify(&Ok::<Option<()>, Error>(None)),
            Attempt::Skipped(Skip::NoLongerDue)
        );
        let unsupported: intent_core::Result<Option<()>> = Err(Error::RetireUnsupported {
            workspace_id: "ws-1".to_string(),
            checkout_mode: None,
            plain_folder: true,
        });
        assert_eq!(classify(&unsupported), Attempt::Skipped(Skip::Unsupported));
        for err in [
            Error::Internal("disk".to_string()),
            Error::InvalidInput("unarchived while retiring".to_string()),
            Error::InvalidParams("pending delete".to_string()),
        ] {
            assert_eq!(classify(&Err::<Option<()>, _>(err)), Attempt::Failed);
        }
    }
}
