//! Quota lock rows (Postgres): the guard of a count-then-insert.
//!
//! A write that admits one more row under a cap (an agent's active PR
//! monitors, the tenant's outstanding invite nonces) runs in one writer
//! transaction: it locks its `quota_lock` row, counts the quota's live rows,
//! refuses at the cap, then writes. Two processes of a tenant admitting
//! under the same quota take the same row lock, so the second counts after
//! the first committed and never both pass at cap − 1.
//!
//! The row holds no count: the count is read from the quota's own rows
//! under the lock, so a missed decrement or a crash cannot make it drift.
//! The tenant writer lock serializes a tenant's writes today as well; the
//! quota row keeps the guard exact if that lock narrows.

use intent_core::Result;

use super::{db, WriteTx};

/// The quota a [`lock_quota`] row guards.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuotaScope {
    /// One agent's active PR monitors; the key is the agent id.
    PrMonitor,
    /// The tenant's outstanding invite nonces; the key is `''`.
    InviteNonce,
}

impl QuotaScope {
    fn as_str(self) -> &'static str {
        match self {
            Self::PrMonitor => "pr-monitor",
            Self::InviteNonce => "invite-nonce",
        }
    }
}

/// Seeds the (tenant, scope, key) row if it has none, then locks it for the
/// rest of `tx`. A concurrent seed of the same row waits on the primary key
/// and then does nothing; the lock below then waits for its transaction.
pub(crate) async fn lock_quota(
    tx: &mut WriteTx,
    tenant: &str,
    scope: QuotaScope,
    key: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO quota_lock (tenant_id, scope, lock_key) VALUES ($1, $2, $3) \
         ON CONFLICT (tenant_id, scope, lock_key) DO NOTHING",
    )
    .bind(tenant)
    .bind(scope.as_str())
    .bind(key)
    .execute(&mut **tx)
    .await
    .map_err(db)?;
    sqlx::query(
        "SELECT 1 FROM quota_lock WHERE tenant_id = $1 AND scope = $2 AND lock_key = $3 \
         FOR UPDATE",
    )
    .bind(tenant)
    .bind(scope.as_str())
    .bind(key)
    .fetch_one(&mut **tx)
    .await
    .map_err(db)?;
    Ok(())
}

/// The agent's active PR monitors, counted in `tx` (after
/// [`lock_quota`] with [`QuotaScope::PrMonitor`] for that agent).
pub(crate) async fn active_pr_monitors(tx: &mut WriteTx, tenant: &str, agent: &str) -> Result<u64> {
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pr_monitor \
         WHERE tenant_id = $1 AND agent_id = $2 AND state = 'active'",
    )
    .bind(tenant)
    .bind(agent)
    .fetch_one(&mut **tx)
    .await
    .map_err(db)?;
    Ok(count.cast_unsigned())
}
