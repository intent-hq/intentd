//! Bounded private backing for profile-specific artifact selectors. This is not
//! a public ordinal selector, native profile validator or physical allocator.
use super::{artifact_lifecycle::identifier, db_error, invalid};
use crate::Store;
use intent_core::{note_artifact::request::ArtifactHeader, Error, Result};
use sqlx::Row;

/// One immutable accepted record, bounded by the append admission limit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactJournalRecord {
    pub sequence: u64,
    pub previous_digest: String,
    pub digest: String,
    pub record: String,
}

impl Store {
    /// Read one accepted record through an active consumer lease. A trusted
    /// profile selector supplies its indexed ordinal; no preceding records are
    /// enumerated. A main writer guard keeps source authority stable through the
    /// arena read transaction; runtime expiry is rechecked before returning bytes.
    /// This internal seam does not establish native index/profile completeness
    /// or expose an artifact RPC. Its caller owns response/frame admission.
    ///
    /// # Errors
    /// Rejects invalid or substituted handles, retired leases, stale source,
    /// restart, expiry and database failures. A missing ordinal returns `None`.
    pub async fn read_note_artifact_journal_record(
        &self,
        principal: &str,
        workspace_id: &str,
        artifact_ref: &str,
        sequence: u64,
    ) -> Result<Option<ArtifactJournalRecord>> {
        identifier(principal)?;
        identifier(workspace_id)?;
        if sequence > 9_007_199_254_740_991 {
            return Err(invalid());
        }
        let token = self.note_pages.decode(artifact_ref)?;
        let lease = token.2.strip_prefix("l:").ok_or_else(invalid)?;
        if token.1 != "r" || token.3 != 0 || uuid::Uuid::parse_str(lease).is_err() {
            return Err(invalid());
        }
        let mut source_guard = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_error)?;
        let mut tx = self.artifact_pool()?.begin().await.map_err(db_error)?;
        let job = sqlx::query("SELECT j.generation,j.header,j.runtime_id,j.expires_at FROM note_artifact_lease l JOIN note_artifact_job j ON j.generation=l.generation WHERE l.lease_id=? AND l.released=0 AND j.principal=? AND j.workspace_id=? AND j.source_snapshot=? AND j.state='admitted' AND j.cleanup_complete=0 AND l.final_digest=j.current_digest AND l.expires_at=j.expires_at")
            .bind(lease).bind(principal).bind(workspace_id).bind(&token.0)
            .fetch_optional(&mut *tx).await.map_err(db_error)?
            .ok_or_else(|| Error::NotFound("Artifact lease is unavailable".into()))?;
        let expires: i64 = job.try_get("expires_at").map_err(db_error)?;
        if job.try_get::<String, _>("runtime_id").map_err(db_error)? != self.note_pages.id
            || expires <= i64::try_from(intent_core::now_epoch_ms()).map_err(|_| invalid())?
        {
            return Err(invalid());
        }
        let header: ArtifactHeader = serde_json::from_str(job.try_get("header").map_err(db_error)?)
            .map_err(|_| Error::Internal("Invalid stored artifact header".into()))?;
        let source = self
            .authorize_artifact_source_in(&mut source_guard, workspace_id, principal, &header)
            .await?;
        let generation: String = job.try_get("generation").map_err(db_error)?;
        // Exact primary-key lookup: no artifact hydration, offset or prefix scan.
        let row = sqlx::query("SELECT previous_digest,digest,record FROM note_artifact_record WHERE generation=? AND sequence=?")
            .bind(generation).bind(super::signed(sequence)?)
            .fetch_optional(&mut *tx).await.map_err(db_error)?;
        let record = row
            .map(|row| -> Result<ArtifactJournalRecord> {
                Ok(ArtifactJournalRecord {
                    sequence,
                    previous_digest: row.try_get("previous_digest").map_err(db_error)?,
                    digest: row.try_get("digest").map_err(db_error)?,
                    record: row.try_get("record").map_err(db_error)?,
                })
            })
            .transpose()?;
        Self::commit_artifact_with_source_guard(source_guard, tx, source.clone()).await?;
        self.note_pages.snapshot(
            &source.snapshot_id,
            workspace_id,
            &source.scope.note_id,
            principal,
        )?;
        if expires <= i64::try_from(intent_core::now_epoch_ms()).map_err(|_| invalid())? {
            return Err(invalid());
        }
        Ok(record)
    }
}
