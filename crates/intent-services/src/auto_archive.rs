//! Automatic archive (PROTOCOL §5.1 "Automatic archive", §5.12
//! `workspace.autoArchiveIdleAfterDays`): a periodic sweep that archives
//! every workspace idle longer than the configured number of days, through
//! the same path and `workspace:updated` delta as `workspace.archive`.
//!
//! The setting is read live at every sweep (absent = 7, `0` = off). A
//! workspace is idle when the newest of `lastContentActivity`,
//! `lastActivity` and `createdAt` is older than the cutoff. Skipped: the
//! Chief workspace, remote workspaces, workspaces that are already archived,
//! retired or deleted, a pending grace-window delete, guest-shared
//! workspaces (archiving would remove the guests), and any workspace with
//! work in flight — a running agent turn, a running script or background
//! worker, an active hook or an active PR monitor. Each workspace is
//! archived independently: a failure is logged and counted, never aborts the
//! sweep. Archived rows are not due any more, so a sweep is idempotent.

use std::time::Duration;

use intent_core::{Workspace, WorkspaceActivity, WorkspaceListFilter, WorkspaceStatus};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::Services;

/// Time between sweeps. The window is configured in days.
const SWEEP_INTERVAL: Duration = Duration::from_secs(3600);

/// Delay before the first sweep, so it stays clear of the startup burst (and
/// of the auto-retire sweep's first run).
const FIRST_SWEEP_DELAY: Duration = Duration::from_secs(180);

/// What one auto-archive sweep did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AutoArchiveReport {
    /// Workspaces archived by this sweep.
    pub archived: usize,
    /// Idle workspaces skipped (remote, guest-shared, pending delete, or
    /// work in flight).
    pub skipped: usize,
    /// Workspaces whose archive attempt failed (logged as a warning;
    /// retried next sweep).
    pub failed: usize,
}

/// The newest parseable idle-clock timestamp of `ws`: `lastContentActivity`,
/// `lastActivity` or `createdAt`. `None` when none parses.
fn last_activity_at(ws: &Workspace) -> Option<OffsetDateTime> {
    [
        ws.last_content_activity.as_deref(),
        ws.last_activity.as_deref(),
        Some(ws.created_at.as_str()),
    ]
    .into_iter()
    .flatten()
    .filter_map(|stamp| OffsetDateTime::parse(stamp, &Rfc3339).ok())
    .max()
}

/// Whether `ws` is an archivable lifecycle state and has been idle since
/// before `cutoff`. A workspace with no parseable timestamp is never due.
pub(crate) fn auto_archive_due(ws: &Workspace, cutoff: OffsetDateTime) -> bool {
    if ws.id.is_chief()
        || ws.is_remote
        || ws.archived
        || matches!(
            ws.status,
            WorkspaceStatus::Archived | WorkspaceStatus::Retired | WorkspaceStatus::Deleted
        )
    {
        return false;
    }
    last_activity_at(ws).is_some_and(|last| last < cutoff)
}

impl Services {
    /// One sweep at `now`, reading `workspace.autoArchiveIdleAfterDays`
    /// from the live settings snapshot. A no-op when the setting is `0`.
    pub(crate) async fn sweep_auto_archive_at(&self, now: OffsetDateTime) -> AutoArchiveReport {
        let days = crate::settings::auto_archive_idle_after_days(&self.effective_settings());
        self.sweep_auto_archive_after(days, now).await
    }

    /// One sweep at `now` with an explicit window: archive every eligible
    /// workspace idle since before `now − days`. `None` does nothing.
    pub(crate) async fn sweep_auto_archive_after(
        &self,
        days: Option<u32>,
        now: OffsetDateTime,
    ) -> AutoArchiveReport {
        let mut report = AutoArchiveReport::default();
        let Some(days) = days else {
            return report;
        };
        let Some(cutoff) = now.checked_sub(time::Duration::days(i64::from(days))) else {
            return report;
        };
        let workspaces = match self
            .store
            .list_workspaces(WorkspaceListFilter::ACTIVE)
            .await
        {
            Ok(list) => list,
            Err(e) => {
                tracing::warn!(error = %e, "auto-archive: listing workspaces failed");
                return report;
            }
        };
        for ws in workspaces {
            if !auto_archive_due(&ws, cutoff) {
                continue;
            }
            if let Some(reason) = self.auto_archive_blocker(&ws).await {
                tracing::debug!(workspace = %ws.id, reason, "auto-archive: skipped");
                report.skipped += 1;
                continue;
            }
            // The listed row may be stale: re-read right before archiving so
            // a message or a manual archive since the listing wins.
            match self.store.get_workspace(&ws.id).await {
                Ok(fresh) if auto_archive_due(&fresh, cutoff) => {}
                Ok(_) => continue,
                Err(e) => {
                    tracing::warn!(workspace = %ws.id, error = %e, "auto-archive: re-read failed");
                    report.failed += 1;
                    continue;
                }
            }
            if self.workspace_activity(&ws.id) == WorkspaceActivity::AgentRunning {
                report.skipped += 1;
                continue;
            }
            match intent_core::WorkspaceApi::archive_workspace(self, ws.id.clone(), None).await {
                Ok(_) => {
                    report.archived += 1;
                    tracing::info!(
                        workspace = %ws.id,
                        idle_after_days = days,
                        "auto-archive: archived idle workspace"
                    );
                }
                Err(e) => {
                    report.failed += 1;
                    tracing::warn!(workspace = %ws.id, error = %e, "auto-archive: archive failed");
                }
            }
        }
        report
    }

    /// Why an idle workspace must not be archived automatically right now;
    /// `None` = archive it. A failed lookup blocks (retried next sweep).
    async fn auto_archive_blocker(&self, ws: &Workspace) -> Option<&'static str> {
        if self.workspace_activity(&ws.id) == WorkspaceActivity::AgentRunning {
            return Some("agent running");
        }
        if self.workspace_scripts_running(&ws.id) {
            return Some("script running");
        }
        if self.agent_workers().workspace_has_running_workers(&ws.id) {
            return Some("background task running");
        }
        if !matches!(
            self.pending_workspace_deletes.deadline(
                &crate::delete_grace::PendingDeleteSubject::Workspace(ws.id.clone())
            ),
            Ok(None)
        ) {
            return Some("pending delete");
        }
        match self.store.count_workspace_guests(&ws.id).await {
            Ok(guests) if guests.collaborators == 0 && guests.open_invites == 0 => {}
            Ok(_) => return Some("shared with guests"),
            Err(_) => return Some("guest lookup failed"),
        }
        match self.store.count_active_hooks_by_workspace(&ws.id).await {
            Ok(0) => {}
            Ok(_) => return Some("active hook"),
            Err(_) => return Some("hook lookup failed"),
        }
        match self
            .store
            .list_active_pr_monitors_by_workspace(&ws.id)
            .await
        {
            Ok(monitors) if monitors.is_empty() => {}
            Ok(_) => return Some("active PR monitor"),
            Err(_) => return Some("PR monitor lookup failed"),
        }
        None
    }

    /// Spawn the auto-archive loop: a first sweep shortly after startup,
    /// then one every hour, each reading the setting live (so changing it
    /// from Settings applies within the hour, without a restart). Stops when
    /// the settings task group closes; the composition root also aborts it
    /// on shutdown.
    #[must_use]
    pub fn spawn_auto_archive_loop(&self) -> tokio::task::JoinHandle<()> {
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
                    .sweep_auto_archive_at(OffsetDateTime::now_utc())
                    .await;
                if report != AutoArchiveReport::default() {
                    tracing::info!(
                        archived = report.archived,
                        skipped = report.skipped,
                        failed = report.failed,
                        "auto-archive: sweep complete"
                    );
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::auto_archive_due;
    use intent_core::{Workspace, WorkspaceId, WorkspaceStatus};
    use time::format_description::well_known::Rfc3339;
    use time::OffsetDateTime;

    fn ts(s: &str) -> OffsetDateTime {
        OffsetDateTime::parse(s, &Rfc3339).unwrap()
    }

    fn ws(created: &str, activity: Option<&str>, content: Option<&str>) -> Workspace {
        Workspace {
            id: WorkspaceId::new(),
            created_at: created.to_string(),
            last_activity: activity.map(str::to_string),
            last_content_activity: content.map(str::to_string),
            ..Workspace::default()
        }
    }

    /// The idle clock is the newest of content activity, activity and
    /// creation; any of them after the cutoff keeps the workspace active.
    #[test]
    fn idle_clock_uses_the_newest_timestamp() {
        let cutoff = ts("2026-10-20T00:00:00Z");
        assert!(auto_archive_due(
            &ws("2026-10-01T00:00:00Z", None, None),
            cutoff
        ));
        assert!(auto_archive_due(
            &ws(
                "2026-09-01T00:00:00Z",
                Some("2026-10-10T00:00:00Z"),
                Some("2026-10-12T00:00:00Z")
            ),
            cutoff
        ));
        assert!(
            !auto_archive_due(&ws("2026-10-21T00:00:00Z", None, None), cutoff),
            "new workspace"
        );
        assert!(!auto_archive_due(
            &ws("2026-09-01T00:00:00Z", Some("2026-10-25T00:00:00Z"), None),
            cutoff
        ));
        assert!(!auto_archive_due(
            &ws("2026-09-01T00:00:00Z", None, Some("2026-10-25T00:00:00Z")),
            cutoff
        ));
        // Unparseable stamps are ignored; with none parseable it is never due.
        assert!(auto_archive_due(
            &ws("2026-09-01T00:00:00Z", Some("garbage"), None),
            cutoff
        ));
        assert!(!auto_archive_due(&ws("garbage", None, None), cutoff));
    }

    /// Only live, local, non-Chief workspaces are ever due.
    #[test]
    fn only_live_local_workspaces_are_due() {
        let cutoff = ts("2026-10-20T00:00:00Z");
        let old = || ws("2026-01-01T00:00:00Z", None, None);
        let mut remote = old();
        remote.is_remote = true;
        assert!(!auto_archive_due(&remote, cutoff));
        let mut archived = old();
        archived.archived = true;
        archived.status = WorkspaceStatus::Archived;
        assert!(!auto_archive_due(&archived, cutoff));
        for status in [WorkspaceStatus::Retired, WorkspaceStatus::Deleted] {
            let mut gone = old();
            gone.status = status;
            assert!(!auto_archive_due(&gone, cutoff), "{status:?}");
        }
        let mut inactive = old();
        inactive.status = WorkspaceStatus::Inactive;
        assert!(auto_archive_due(&inactive, cutoff));
        let mut chief = old();
        chief.id = WorkspaceId::from(intent_core::CHIEF_WORKSPACE_ID);
        assert!(!auto_archive_due(&chief, cutoff));
    }
}
