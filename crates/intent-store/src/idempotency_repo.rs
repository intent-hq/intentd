//! Idempotency-key dedupe repository (design note TB-0 §5.1). Keyed by
//! `(workspace_id, idempotency_key)` — `workspace_id` is the `""` sentinel for
//! global methods (e.g. `workspace.create`). Stores the serialized original
//! result so a replayed create/commit/PR-merge returns it without re-executing.
//! Backs the `intent-services` `with_idempotency` wrapper and the ~hourly reaper.

use intent_core::{now_iso, Error, Result};
use sqlx::Row;

use crate::Store;

/// What [`Store::reserve_idempotent`] found for a key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdempotencyReservation {
    /// This process reserved the key: run the operation, then
    /// [`Store::complete_idempotent`] (or [`Store::release_idempotent`] when
    /// it fails).
    Reserved,
    /// The operation already ran: replay its stored `result_json`.
    Done(String),
    /// Another process holds a live reservation of the key and is still
    /// running the operation (Postgres only): wait and look again.
    InProgress,
}

/// How long a Postgres reservation stays live without a renewal
/// ([`Store::renew_idempotent`]). A holder renews well within it while its
/// operation runs; a holder that died frees the key for a retry in another
/// process within this time.
pub const IDEMPOTENCY_CLAIM_TTL: std::time::Duration = std::time::Duration::from_secs(20);

impl Store {
    /// Reserve `(workspace_id, key)` before running a keyed operation, so a
    /// retry in another process cannot run it a second time (batch G 2b).
    /// On Postgres a reservation is a `pending` row naming this process and
    /// an expiry by the database clock ([`IDEMPOTENCY_CLAIM_TTL`]); another
    /// process may take it over only once it expired, also across a tenant
    /// takeover. `SQLite` (one process) reports the stored result or
    /// `Reserved`.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotOwner` when this process lost the tenant, and
    /// `Error::Internal` if the database operation fails.
    pub async fn reserve_idempotent(
        &self,
        workspace_id: &str,
        key: &str,
        method: &str,
    ) -> Result<IdempotencyReservation> {
        dispatch!(self, backend => backend.reserve_idempotent(workspace_id, key, method).await)
    }

    /// Store the result of an operation this process reserved; it is then
    /// replayed to every later caller with the key.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn complete_idempotent(
        &self,
        workspace_id: &str,
        key: &str,
        method: &str,
        result_json: &str,
    ) -> Result<()> {
        dispatch!(self, backend => backend.complete_idempotent(workspace_id, key, method, result_json).await)
    }

    /// Restart the expiry of this process's reservation while its operation
    /// runs. `false` when the reservation is gone: another process took it
    /// over after it expired, so this run's result will not be stored.
    /// Always `true` on `SQLite`.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotOwner` when this process lost the tenant, and
    /// `Error::Internal` if the database operation fails.
    pub async fn renew_idempotent(&self, workspace_id: &str, key: &str) -> Result<bool> {
        dispatch!(self, backend => backend.renew_idempotent(workspace_id, key).await)
    }

    /// Drop this process's reservation after the operation failed, so a
    /// retry may run it (nothing is stored for a failed operation).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn release_idempotent(&self, workspace_id: &str, key: &str) -> Result<()> {
        dispatch!(self, backend => backend.release_idempotent(workspace_id, key).await)
    }

    /// Look up the stored `result_json` for `(workspace_id, key)`, or `None` when
    /// the key has not been recorded yet.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn get_idempotent(&self, workspace_id: &str, key: &str) -> Result<Option<String>> {
        dispatch!(self, backend => backend.get_idempotent(workspace_id, key).await)
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
        dispatch!(self, backend => backend.put_idempotent(workspace_id, key, method, result_json).await)
    }

    /// Reaper sweep (design §5.4): delete rows whose `created_at` is strictly
    /// older than `cutoff` (an RFC-3339 string), returning the number removed.
    /// Uses `idx_idempotency_created`; idempotent — a re-run removes nothing more.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn reap_idempotent(&self, cutoff: &str) -> Result<u64> {
        dispatch!(self, backend => backend.reap_idempotent(cutoff).await)
    }
}

impl crate::SqliteStore {
    // One process per database file: the in-process key lock orders callers,
    // so a reservation is just the lookup.
    pub async fn reserve_idempotent(
        &self,
        workspace_id: &str,
        key: &str,
        _method: &str,
    ) -> Result<IdempotencyReservation> {
        Ok(match self.get_idempotent(workspace_id, key).await? {
            Some(stored) => IdempotencyReservation::Done(stored),
            None => IdempotencyReservation::Reserved,
        })
    }

    pub async fn complete_idempotent(
        &self,
        workspace_id: &str,
        key: &str,
        method: &str,
        result_json: &str,
    ) -> Result<()> {
        self.put_idempotent(workspace_id, key, method, result_json)
            .await
            .map(|_| ())
    }

    #[expect(clippy::unused_async, reason = "mirrors the Postgres reservation")]
    pub async fn renew_idempotent(&self, _workspace_id: &str, _key: &str) -> Result<bool> {
        Ok(true)
    }

    #[expect(clippy::unused_async, reason = "mirrors the Postgres reservation")]
    pub async fn release_idempotent(&self, _workspace_id: &str, _key: &str) -> Result<()> {
        Ok(())
    }

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

    pub async fn reap_idempotent(&self, cutoff: &str) -> Result<u64> {
        let res = sqlx::query("DELETE FROM idempotency_key WHERE created_at < ?")
            .bind(cutoff)
            .execute(self.write_pool())
            .await
            .map_err(|e| Error::Internal(format!("idempotency reap failed: {e}")))?;
        Ok(res.rows_affected())
    }
}
