//! The per-agent PR-monitor quota (`prMonitor.maxPerAgent`), checked in the
//! write that admits a monitor (`quota.rs` states the mechanism). Every
//! write that can raise an agent's active monitors has a capped form: a
//! legacy insert, a legacy adoption (re-arm under a new owner), and the
//! prepared insert or ownership transfer
//! ([`PreparedMonitorWrite::admit_under_quota`]). A re-arm by the same
//! owner adds nothing and is never refused.
use super::{AgentId, MonitorTargetProvenance, PrMonitor, Result, ReviewTarget, Store};
use crate::QuotaWrite;

impl Store {
    /// [`Store::insert_qualified_pr_monitor`], admitted under the owner's
    /// quota: in the same write transaction, count `m.agent_id`'s active
    /// monitors under its quota lock and insert only below `max`
    /// ([`QuotaWrite::Full`] otherwise). A uniqueness conflict is
    /// [`QuotaWrite::Lost`].
    ///
    /// # Errors
    /// Returns `InvalidParams` for inconsistent data and `Internal` on database failure.
    pub async fn insert_qualified_pr_monitor_within_quota(
        &self,
        m: &PrMonitor,
        target: &ReviewTarget,
        provenance: MonitorTargetProvenance,
        max: u64,
    ) -> Result<QuotaWrite> {
        dispatch!(self, backend => backend
            .insert_qualified_pr_monitor_within_quota(m, target, provenance, Some(max))
            .await)
    }

    /// [`Store::rearm_legacy_pr_monitor`], admitted under `owner`'s quota
    /// when it changes the row's owner (an adoption): `owner`'s active
    /// monitors are counted under its quota lock in the same write
    /// transaction, and the adoption lands only below `max`. A re-arm by
    /// the row's own owner is never refused. A lost compare-and-swap is
    /// [`QuotaWrite::Lost`].
    ///
    /// # Errors
    /// As [`Store::rearm_legacy_pr_monitor`].
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
        max: u64,
    ) -> Result<QuotaWrite> {
        let max = (expected.agent_id != *owner).then_some(max);
        dispatch!(self, backend => backend
            .rearm_legacy_pr_monitor_within_quota(expected, owner, target, baseline, now, pause, max)
            .await)
    }
}

/// `agent`'s active monitors, counted in a `SQLite` write transaction.
pub(crate) async fn sqlite_active_monitors(
    conn: &mut sqlx::SqliteConnection,
    agent: &AgentId,
) -> Result<u64> {
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pr_monitor WHERE agent_id = ? AND state = 'active'",
    )
    .bind(&agent.0)
    .fetch_one(conn)
    .await
    .map_err(|e| intent_core::Error::Internal(format!("count active monitors: {e}")))?;
    Ok(n.cast_unsigned())
}
