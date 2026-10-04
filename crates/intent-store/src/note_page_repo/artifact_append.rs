//! Exact-record logical journal writes. Native profile validation and physical
//! allocation remain separate trusted service obligations, not wire parameters.
use super::{
    artifact_lifecycle::{identifier, status, STATUS_COLUMNS},
    db_error, invalid, ArtifactJournalStatus,
};
use crate::Store;
use intent_core::{
    note_artifact::request::{ArtifactAppend, ArtifactHeader, SAFE_INTEGER},
    Error, Result,
};
use sqlx::Row;

/// Trusted profile/index-writer result for the exact validated record. The
/// caller must reserve physical storage separately and include journal/ACK
/// overhead in its measured charge. These fields are never client authority.
pub struct ArtifactJournalRecordCost {
    pub index_entries: u64,
    pub storage_bytes: u64,
    pub final_manifest: bool,
}

impl Store {
    /// Atomically append a bounded record or return its original acknowledgement.
    /// This prepared repository seam does not register a profile, publish an
    /// artifact, or expose an RPC. The service must validate the exact native
    /// record and admit physical allocation before supplying `cost`.
    ///
    /// # Errors
    /// Rejects invalid integrity, stale source/principal, changed replay, sequence
    /// gaps, exhausted logical reservations, or database failures.
    pub async fn append_note_artifact_journal(
        &self,
        principal: &str,
        workspace_id: &str,
        request: &ArtifactAppend,
        cost: &ArtifactJournalRecordCost,
    ) -> Result<ArtifactJournalStatus> {
        identifier(principal)?;
        identifier(workspace_id)?;
        // Enforce the byte/structural ceiling before taking a writer credit.
        intent_core::note_artifact::preflight_record(&request.record).map_err(|_| invalid())?;
        if cost.index_entries > SAFE_INTEGER || cost.storage_bytes > SAFE_INTEGER {
            return Err(invalid());
        }
        let token = self.note_pages.decode(&request.job_ref)?;
        let generation = token.2.strip_prefix("j:").ok_or_else(invalid)?;
        if token.1 != "r" || token.3 != 0 || uuid::Uuid::parse_str(generation).is_err() {
            return Err(invalid());
        }
        let mut source_guard = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_error)?;
        let mut tx = self
            .artifact_pool()?
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_error)?;
        let query = format!("SELECT {STATUS_COLUMNS},header,runtime_id FROM note_artifact_job WHERE generation=? AND principal=? AND workspace_id=? AND source_snapshot=?");
        let row = sqlx::query(&query)
            .bind(generation)
            .bind(principal)
            .bind(workspace_id)
            .bind(&token.0)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db_error)?
            .ok_or_else(|| Error::NotFound("Artifact job not found".into()))?;
        let mut receipt = status(&self.note_pages, &row)?;
        request
            .validate(&receipt.header_digest)
            .map_err(|_| invalid())?;
        if row.try_get::<String, _>("runtime_id").map_err(db_error)? != self.note_pages.id
            || receipt.expires_at
                <= i64::try_from(intent_core::now_epoch_ms()).map_err(|_| invalid())?
        {
            return Err(invalid());
        }
        let header: ArtifactHeader = serde_json::from_str(row.try_get("header").map_err(db_error)?)
            .map_err(|_| Error::Internal("Invalid stored artifact header".into()))?;
        let source = self
            .authorize_artifact_source_in(&mut source_guard, workspace_id, principal, &header)
            .await?;
        let sequence = super::signed(request.sequence)?;
        let old = sqlx::query("SELECT r.previous_digest,r.digest,r.record,a.accepted_bytes FROM note_artifact_record r JOIN note_artifact_ack a USING(generation,sequence) WHERE r.generation=? AND r.sequence=?")
            .bind(generation).bind(sequence).fetch_optional(&mut *tx).await.map_err(db_error)?;
        if let Some(old) = old {
            if old
                .try_get::<String, _>("previous_digest")
                .map_err(db_error)?
                != request.previous_digest
                || old.try_get::<String, _>("digest").map_err(db_error)? != request.digest
                || old.try_get::<String, _>("record").map_err(db_error)? != request.record
            {
                return Err(Error::InvalidParams(
                    "Artifact record replay mismatch".into(),
                ));
            }
            receipt.accepted_bytes = old.try_get("accepted_bytes").map_err(db_error)?;
        } else {
            sqlx::query("INSERT INTO note_artifact_record(generation,sequence,previous_digest,digest,record,index_charge,storage_charge,is_manifest) VALUES (?,?,?,?,?,?,?,?)")
                .bind(generation).bind(sequence).bind(&request.previous_digest).bind(&request.digest)
                .bind(&request.record).bind(super::signed(cost.index_entries)?).bind(super::signed(cost.storage_bytes)?)
                .bind(cost.final_manifest).execute(&mut *tx).await.map_err(db_error)?;
            receipt.accepted_bytes = sqlx::query_scalar(
                "SELECT accepted_bytes FROM note_artifact_ack WHERE generation=? AND sequence=?",
            )
            .bind(generation)
            .bind(sequence)
            .fetch_one(&mut *tx)
            .await
            .map_err(db_error)?;
        }
        // Return the original accepted prefix, even if this is a retry after
        // later records/seal. Current status remains separately queryable.
        receipt.next_sequence = sequence.checked_add(1).ok_or_else(invalid)?;
        receipt.current_digest.clone_from(&request.digest);
        receipt.state = "building".into();
        receipt.cleanup_complete = false;
        receipt.private_artifact_ref = None;
        self.note_pages.snapshot(
            &source.snapshot_id,
            workspace_id,
            &source.scope.note_id,
            principal,
        )?;
        if receipt.expires_at
            <= i64::try_from(intent_core::now_epoch_ms()).map_err(|_| invalid())?
        {
            return Err(invalid());
        }
        Self::commit_artifact_with_source_guard(source_guard, tx, source).await?;
        Ok(receipt)
    }
}
