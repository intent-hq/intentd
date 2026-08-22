//! Idempotency-key dedupe repository (design note TB-0 §5.1). Keyed by
//! `(workspace_id, idempotency_key)` — `workspace_id` is the `""` sentinel for
//! global methods (e.g. `workspace.create`). Stores the serialized original
//! result so a replayed create/commit/PR-merge returns it without re-executing.
//! Backs the `intent-services` `with_idempotency` wrapper and the ~hourly reaper.

use intent_core::{now_iso, Error, Result};
use sqlx::Row;

use crate::Store;

impl Store {
    /// Look up the stored `result_json` for `(workspace_id, key)`, or `None` when
    /// the key has not been recorded yet.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn get_idempotent(&self, workspace_id: &str, key: &str) -> Result<Option<String>> {
        let row = sqlx::query(
            "SELECT result_json FROM idempotency_key WHERE workspace_id = ? AND idempotency_key = ?",
        )
        .bind(workspace_id)
        .bind(key)
        .fetch_optional(self.read_pool())
        .await
        .map_err(|e| Error::Internal(format!("idempotency lookup failed: {e}")))?;
        Ok(row.map(|r| r.get::<String, _>("result_json")))
    }

    /// Record the serialized `result_json` under `(workspace_id, key)`. Uses
    /// `INSERT OR IGNORE` so a concurrent duplicate that won the race is kept and
    /// the loser is a no-op (best-effort dedupe, design §5.3). Returns `true` when
    /// this call inserted the row.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn put_idempotent(
        &self,
        workspace_id: &str,
        key: &str,
        method: &str,
        result_json: &str,
    ) -> Result<bool> {
        let res = sqlx::query(
            "INSERT OR IGNORE INTO idempotency_key \
             (workspace_id, idempotency_key, method, result_json, created_at) \
             VALUES (?,?,?,?,?)",
        )
        .bind(workspace_id)
        .bind(key)
        .bind(method)
        .bind(result_json)
        .bind(now_iso())
        .execute(self.write_pool())
        .await
        .map_err(|e| Error::Internal(format!("idempotency insert failed: {e}")))?;
        Ok(res.rows_affected() > 0)
    }

    /// Reaper sweep (design §5.4): delete rows whose `created_at` is strictly
    /// older than `cutoff` (an RFC-3339 string), returning the number removed.
    /// Uses `idx_idempotency_created`; idempotent — a re-run removes nothing more.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn reap_idempotent(&self, cutoff: &str) -> Result<u64> {
        let res = sqlx::query("DELETE FROM idempotency_key WHERE created_at < ?")
            .bind(cutoff)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("idempotency reap failed: {e}")))?;
        Ok(res.rows_affected())
    }
}
