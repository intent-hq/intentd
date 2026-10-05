//! Sharing counters are maintained by 0134's triggers. Time passage is not a
//! mutation: before exposing a count, reconcile due invitation reservations at
//! one observation time. The ordinary path holds a read snapshot and only probes
//! the (`workspace_id`, `expires_at`) index. If due rows exist, restart under the write
//! lock, delete those reservations and read the counters in that same snapshot.
//! Work is O(requested workspaces + due reservations), never unrelated grants or
//! invite history. Admission transactions call the same deletion under their
//! existing write lock. History is untouched; public invitation expiry remains
//! governed by `INVITE_OPEN`. No stale timer/cache can authorize an extra seat.

use intent_core::{now_iso, Error, Result, WorkspaceId};
use sqlx::{Sqlite, SqliteConnection, Transaction};

use crate::Store;

pub(crate) async fn reconcile_invite_expiry(
    conn: &mut SqliteConnection,
    workspace_ids: &[WorkspaceId],
    at: &str,
) -> Result<()> {
    if workspace_ids.is_empty() {
        return Ok(());
    }
    let placeholders = vec!["?"; workspace_ids.len()].join(",");
    let sql=format!("DELETE FROM workspace_invite_seat WHERE workspace_id IN ({placeholders}) AND expires_at <= ?");
    let mut query = sqlx::query(&sql);
    for id in workspace_ids {
        query = query.bind(id.as_str());
    }
    query
        .bind(at)
        .execute(conn)
        .await
        .map_err(|e| Error::Internal(format!("reconcile invitation expiry failed: {e}")))?;
    Ok(())
}

impl Store {
    pub(crate) async fn sharing_snapshot(
        &self,
        ids: &[WorkspaceId],
        at: Option<&str>,
    ) -> Result<Transaction<'_, Sqlite>> {
        let mut tx = self
            .read_pool()
            .begin()
            .await
            .map_err(|e| Error::Internal(format!("sharing snapshot failed: {e}")))?;
        // Production observations take their time after acquiring the
        // connection. Tests may supply an exact boundary without sleeping.
        let observed_at = at.map_or_else(now_iso, str::to_owned);
        let placeholders = vec!["?"; ids.len()].join(",");
        let sql=format!("SELECT 1 FROM workspace_invite_seat WHERE workspace_id IN ({placeholders}) AND expires_at <= ? LIMIT 1");
        let mut query = sqlx::query_scalar::<_, i64>(&sql);
        for id in ids {
            query = query.bind(id.as_str());
        }
        let due = query
            .bind(&observed_at)
            .fetch_optional(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("invitation expiry deadline failed: {e}")))?
            .is_some();
        if !due {
            return Ok(tx);
        }
        tx.rollback()
            .await
            .map_err(|e| Error::Internal(format!("sharing snapshot restart failed: {e}")))?;
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|e| Error::Internal(format!("sharing expiry snapshot failed: {e}")))?;
        // Waiting for a writer can itself cross a deadline; refresh the
        // observation boundary under the acquired write lock.
        let observed_at = at.map_or_else(now_iso, str::to_owned);
        reconcile_invite_expiry(&mut tx, ids, &observed_at).await?;
        Ok(tx)
    }
}
