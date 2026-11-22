//! Persisted delete grace deadlines (batch G, step 3). A `*.delete` with
//! `undoDelayMs > 0` arms an in-memory timer (`intent-services`
//! `delete_grace`). On Postgres (orchestrator mode) the deadline is also
//! stored, so a delete in its grace period survives a restart or a takeover
//! by another process and commits once: the process that commits it first
//! takes the row ([`Store::take_delete_grace`]); a process that finds the
//! row gone skips. `SQLite` (desktop) keeps the documented behaviour: a
//! restart drops pending deletions, so nothing is stored there.

use intent_core::Result;

use crate::Store;

/// One stored grace deadline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteGrace {
    /// `"workspace"` or `"agent"`.
    pub subject_kind: String,
    /// The workspace or agent id.
    pub subject_id: String,
    /// The workspace (for an agent, the one it belongs to).
    pub workspace_id: String,
    /// ISO deadline at which the delete commits.
    pub delete_at: String,
}

impl Store {
    /// Records `grace` unless a deadline for the same subject is already
    /// stored; returns the stored deadline (the earlier one wins, so a
    /// re-schedule never extends it). `SQLite` stores nothing and returns
    /// `grace.delete_at`.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotOwner` when this process lost the tenant, and
    /// `Error::Internal` if the database operation fails.
    pub async fn record_delete_grace(&self, grace: &DeleteGrace) -> Result<String> {
        dispatch!(self, backend => backend.record_delete_grace(grace).await)
    }

    /// Takes the stored deadline of a subject: `true` when this call removed
    /// it, `false` when it was already gone (cancelled, or committed by
    /// another process). A commit runs only on `true`, so it runs once.
    /// `SQLite` always returns `true`.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotOwner` when this process lost the tenant, and
    /// `Error::Internal` if the database operation fails.
    pub async fn take_delete_grace(&self, subject_kind: &str, subject_id: &str) -> Result<bool> {
        dispatch!(self, backend => backend.take_delete_grace(subject_kind, subject_id).await)
    }

    /// Every stored deadline, for a process to re-arm at startup. `SQLite`
    /// returns none.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn list_delete_graces(&self) -> Result<Vec<DeleteGrace>> {
        dispatch!(self, backend => backend.list_delete_graces().await)
    }
}

// One daemon per database file: a restart drops pending deletions (the
// documented desktop behaviour), so nothing is stored.
impl crate::SqliteStore {
    #[expect(clippy::unused_async, reason = "same signature as the Postgres store")]
    pub async fn record_delete_grace(&self, grace: &DeleteGrace) -> Result<String> {
        Ok(grace.delete_at.clone())
    }

    #[expect(clippy::unused_async, reason = "same signature as the Postgres store")]
    pub async fn take_delete_grace(&self, _subject_kind: &str, _subject_id: &str) -> Result<bool> {
        Ok(true)
    }

    #[expect(clippy::unused_async, reason = "same signature as the Postgres store")]
    pub async fn list_delete_graces(&self) -> Result<Vec<DeleteGrace>> {
        Ok(Vec::new())
    }
}
