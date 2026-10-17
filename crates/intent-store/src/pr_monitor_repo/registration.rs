//! Explicit GitHub registration repairs old rows atomically with their baseline.
use super::{
    pause_preserving_last_error_sql, validate_monitor_target, AgentId, MonitorTargetProvenance,
    MonitorTargetUnresolvedReason, PersistedMonitorTarget, PrMonitor, Result, ReviewTarget, Store,
    PR_MONITOR_PAUSE_MARKER,
};

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
        validate_monitor_target(
            expected,
            target,
            MonitorTargetProvenance::LegacyGithubWriter,
        )?;
        let captured = self.get_qualified_pr_monitor(&expected.monitor_id).await?;
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
        let mut query = sqlx::query(&sql)
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
        let result = query.execute(self.write_pool()).await.map_err(|error| {
            intent_core::Error::Internal(format!("rearm legacy pr monitor: {error}"))
        })?;
        Ok(result.rows_affected() == 1)
    }
}
