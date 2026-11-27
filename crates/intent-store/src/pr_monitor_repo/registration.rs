//! Explicit GitHub registration repairs old rows atomically with their baseline.
use super::{
    pause_preserving_last_error_sql, validate_monitor_target, AgentId, MonitorTargetProvenance,
    MonitorTargetUnresolvedReason, PersistedMonitorTarget, PrMonitor, QualifiedPrMonitor, Result,
    ReviewTarget, Store, PR_MONITOR_PAUSE_MARKER,
};
use crate::QuotaWrite;

impl Store {
    /// Re-arm or adopt a legacy monitor using the current registration's target.
    /// A pre-target-column row can acquire writer provenance only together with
    /// a fresh baseline (or a cleared, deferred baseline). Old imported snapshots
    /// and pending changes never become eligible for legacy output or polling.
    ///
    /// This is a registration write, not a boot-time inference from a slug.
    /// Imported claims and other qualified routes must be explicitly cancelled
    /// or recovered through their own route instead of being reclassified here.
    /// The caller holds the rate-limit reconciliation lock and supplies its live
    /// pause: unresolved rows were excluded from the earlier bulk annotation.
    ///
    /// # Errors
    /// Returns invalid parameters for incompatible provenance/identity, or a
    /// database error. A concurrent lifecycle/identity change returns false.
    pub async fn rearm_legacy_pr_monitor(
        &self,
        expected: &PrMonitor,
        owner: &AgentId,
        target: &ReviewTarget,
        baseline: Option<&str>,
        now: &str,
        pause: Option<&str>,
    ) -> Result<bool> {
        dispatch!(self, backend => backend.rearm_legacy_pr_monitor(expected, owner, target, baseline, now, pause).await)
    }
}

impl crate::SqliteStore {
    pub async fn rearm_legacy_pr_monitor(
        &self,
        expected: &PrMonitor,
        owner: &AgentId,
        target: &ReviewTarget,
        baseline: Option<&str>,
        now: &str,
        pause: Option<&str>,
    ) -> Result<bool> {
        Ok(self
            .rearm_legacy_pr_monitor_within_quota(
                expected, owner, target, baseline, now, pause, None,
            )
            .await?
            == QuotaWrite::Written)
    }

    /// The re-arm; with `max`, `owner`'s active monitors are counted in its
    /// `BEGIN IMMEDIATE` transaction first.
    #[expect(
        clippy::too_many_arguments,
        reason = "the re-arm's arguments plus the cap"
    )]
    pub async fn rearm_legacy_pr_monitor_within_quota(
        &self,
        expected: &PrMonitor,
        owner: &AgentId,
        target: &ReviewTarget,
        baseline: Option<&str>,
        now: &str,
        pause: Option<&str>,
        max: Option<u64>,
    ) -> Result<QuotaWrite> {
        validate_monitor_target(
            expected,
            target,
            MonitorTargetProvenance::LegacyGithubWriter,
        )?;
        let captured = self.get_qualified_pr_monitor(&expected.monitor_id).await?;
        check_rearm_compatible(&captured)?;
        let sql = format!(
            "UPDATE pr_monitor SET agent_id=?1, last_snapshot=?2, baseline_snapshot=?2,
             pending_changes='[]', pending_since=NULL, last_change_at=NULL, last_polled_at=?3,
             last_error=CASE WHEN target_provenance='unresolved' THEN ?20 ELSE {} END, updated_at=?4,
             target_provider='github', target_instance_base_url='https://github.com',
             target_project_path=?6, target_kind='pull-request',
             target_provenance='legacy-github-writer', target_unresolved_reason=NULL
             WHERE monitor_id=?7 AND agent_id=?8 AND workspace_id=?9
             AND repo_owner=?10 AND repo_name=?11 AND pr_number=?12
             AND state='active' AND updated_at=?13
             AND target_provider IS ?14 AND target_instance_base_url IS ?15
             AND target_project_path IS ?16 AND target_kind IS ?17
             AND target_provenance IS ?18 AND target_unresolved_reason IS ?19",
            pause_preserving_last_error_sql("NULL", "?5", "?4"),
        );
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(&owner.0)
            .bind(baseline)
            .bind(baseline.map(|_| now))
            .bind(now)
            .bind(PR_MONITOR_PAUSE_MARKER)
            .bind(&target.repository.project_path)
            .bind(&expected.monitor_id.0)
            .bind(&expected.agent_id.0)
            .bind(&expected.workspace_id.0)
            .bind(&expected.repo_owner)
            .bind(&expected.repo_name)
            .bind(expected.pr_number)
            .bind(&expected.updated_at);
        for value in captured.columns.values() {
            query = query.bind(value);
        }
        query = query.bind(pause);
        let internal = |error: sqlx::Error| {
            intent_core::Error::Internal(format!("rearm legacy pr monitor: {error}"))
        };
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(internal)?;
        if let Some(max) = max {
            let active = super::quota::sqlite_active_monitors(&mut tx, owner).await?;
            if active >= max {
                return Ok(QuotaWrite::Full { active });
            }
        }
        let result = query.execute(&mut *tx).await.map_err(internal)?;
        tx.commit().await.map_err(internal)?;
        Ok(if result.rows_affected() == 1 {
            QuotaWrite::Written
        } else {
            QuotaWrite::Lost
        })
    }
}

/// Only writer-provenance rows, or pre-target-column rows with no target claim,
/// may be re-armed by a legacy GitHub registration.
pub(crate) fn check_rearm_compatible(captured: &QualifiedPrMonitor) -> Result<()> {
    let compatible = match captured.target() {
        PersistedMonitorTarget::Resolved {
            provenance: MonitorTargetProvenance::LegacyGithubWriter,
            ..
        } => true,
        PersistedMonitorTarget::Unresolved {
            reason: MonitorTargetUnresolvedReason::MissingProvenance,
        } => {
            captured.columns.provider.is_none()
                && captured.columns.instance.is_none()
                && captured.columns.project.is_none()
                && captured.columns.kind.is_none()
        }
        _ => false,
    };
    if !compatible {
        return Err(intent_core::Error::InvalidParams(
            "pr.monitor: existing monitor has incompatible target evidence; use its original repository route or explicitly cancel it before registering again".into(),
        ));
    }
    Ok(())
}
