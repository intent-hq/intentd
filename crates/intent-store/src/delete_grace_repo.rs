//! Persisted delete grace deadlines (batch G, step 3). A `*.delete` with
//! `undoDelayMs > 0` arms an in-memory timer (`intent-services`
//! `delete_grace`). On Postgres (orchestrator mode) the deadline is also
//! stored, so a delete in its grace period survives a restart or a takeover
//! by another process and commits once: the committing process claims the
//! row ([`Store::claim_delete_grace`]), runs the delete, then removes it
//! ([`Store::finish_delete_grace`]); a crash in between leaves a stale claim
//! that the next owner takes over. `SQLite` (desktop) keeps the documented behaviour: a
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
    /// Whether this backend stores grace deadlines (Postgres): only then
    /// does [`Store::take_delete_grace`] decide which process commits.
    #[must_use]
    pub fn persists_delete_graces(&self) -> bool {
        !matches!(self.backend, crate::Backend::Sqlite(_))
    }

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

    /// Claims a stored deadline for its commit, at the tenant's epoch and in
    /// this process's name: `true` when this process may run the delete
    /// (unclaimed, already its claim, or a claim from an older epoch or a
    /// gone process), `false` when the row is gone (cancelled or finished)
    /// or another live process holds it. `SQLite` always returns `true`.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotOwner` when this process lost the tenant, and
    /// `Error::Internal` if the database operation fails.
    pub async fn claim_delete_grace(&self, subject_kind: &str, subject_id: &str) -> Result<bool> {
        dispatch!(self, backend => backend.claim_delete_grace(subject_kind, subject_id).await)
    }

    /// Removes a deadline this process claimed, once its delete ran.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the database operation fails.
    pub async fn finish_delete_grace(&self, subject_kind: &str, subject_id: &str) -> Result<()> {
        dispatch!(self, backend => backend.finish_delete_grace(subject_kind, subject_id).await)
    }

    /// Cancels a pending delete's stored deadline: `true` when this call
    /// removed it, `false` when it is gone or claimed by a commit already
    /// under way. `SQLite` always returns `true`.
    ///
    /// # Errors
    ///
    /// Returns `Error::NotOwner` when this process lost the tenant, and
    /// `Error::Internal` if the database operation fails.
    pub async fn cancel_delete_grace(&self, subject_kind: &str, subject_id: &str) -> Result<bool> {
        dispatch!(self, backend => backend.cancel_delete_grace(subject_kind, subject_id).await)
    }

    /// Removes a stored deadline whatever its claim (an immediate delete
    /// superseded it, or its subject is gone): `true` when a row was
    /// removed. `SQLite` always returns `true`.
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
    pub async fn claim_delete_grace(&self, _subject_kind: &str, _subject_id: &str) -> Result<bool> {
        Ok(true)
    }

    #[expect(clippy::unused_async, reason = "same signature as the Postgres store")]
    pub async fn finish_delete_grace(&self, _subject_kind: &str, _subject_id: &str) -> Result<()> {
        Ok(())
    }

    #[expect(clippy::unused_async, reason = "same signature as the Postgres store")]
    pub async fn cancel_delete_grace(
        &self,
        _subject_kind: &str,
        _subject_id: &str,
    ) -> Result<bool> {
        Ok(true)
    }

    #[expect(clippy::unused_async, reason = "same signature as the Postgres store")]
    pub async fn list_delete_graces(&self) -> Result<Vec<DeleteGrace>> {
        Ok(Vec::new())
    }
}
