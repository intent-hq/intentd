//! Atomic logical journal admission. No renderer profile or physical storage
//! allocator is registered here; the service must admit those before calling.
use super::{
    artifact_lifecycle::{identifier, status, STATUS_COLUMNS},
    db_error, invalid, ArtifactJournalStatus,
};
use crate::Store;
use intent_core::{note_artifact::request::ArtifactBegin, Error, Result};
use sqlx::Row;

impl Store {
    /// Reserve a prepared private journal against the current indexed source.
    /// Finite global, principal and workspace capacity must already be configured.
    /// `status_until` is the trusted server retention deadline in epoch milliseconds,
    /// not a caller-supplied extension of source authority. This is not the public
    /// artifact begin route: renderer/environment admission, a physical storage
    /// reservation and constructor ownership remain the service's obligations.
    ///
    /// # Errors
    /// Rejects invalid integrity/expiry, stale or unauthorized source, conflicting
    /// replay identity, exhausted configured capacity, and database failures.
    pub async fn begin_note_artifact_journal(
        &self,
        principal: &str,
        workspace_id: &str,
        request: &ArtifactBegin,
        status_until: i64,
    ) -> Result<ArtifactJournalStatus> {
        identifier(principal)?;
        identifier(workspace_id)?;
        request.validate(workspace_id).map_err(|_| invalid())?;
        let expiry = intent_core::parse_iso(&request.expires_at).ok_or_else(invalid)?;
        let expires_at =
            i64::try_from(expiry.unix_timestamp_nanos() / 1_000_000).map_err(|_| invalid())?;
        if expires_at <= 0 || status_until < expires_at {
            return Err(invalid());
        }
        let header = serde_json::to_string(&request.header).map_err(|_| invalid())?;
        if header.len() > intent_core::note_artifact::RECORD_BYTES {
            return Err(invalid());
        }
        // A managed transaction also rolls back if this future is cancelled.
        let mut source_guard = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_error)?;
        let mut connection = self
            .artifact_pool()?
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_error)?;
        let result = async {
            let query = format!("SELECT {STATUS_COLUMNS} FROM note_artifact_job WHERE principal=? AND workspace_id=? AND job_id=?");
            let existing = sqlx::query(&query).bind(principal).bind(workspace_id).bind(&request.job_id)
                .fetch_optional(&mut *connection).await.map_err(db_error)?;
            if let Some(row) = &existing {
                if row.try_get::<String, _>("header_digest").map_err(db_error)? != request.header_digest {
                    return Err(Error::InvalidParams("Artifact identity mismatch".into()));
                }
            }
            // Revalidate after obtaining the writer lock, even for an identical
            // replay. An earlier read grant is not mutation authority.
            let source = self.authorize_artifact_source_in(
                &mut source_guard, workspace_id, principal, &request.header,
            ).await?;
            let source_expiry = intent_core::parse_iso(&source.expires_at).ok_or_else(invalid)?;
            let now = intent_core::parse_iso(&intent_core::now_iso()).ok_or_else(invalid)?;
            if expiry <= now || expiry > source_expiry {
                return Err(invalid());
            }
            if let Some(row) = existing {
                // Preserve the original state/receipt and reservation. In
                // particular, aborted/released jobs never become building again.
                return status(&self.note_pages, &row).map(|state| (state, source));
            }
            let generation = uuid::Uuid::new_v4().simple().to_string();
            let reservation = &request.header.reservation;
            sqlx::query("INSERT INTO note_artifact_job(principal,workspace_id,job_id,generation,runtime_id,header_digest,header,source_snapshot,source_revision,note_id,note_instance_id,source_collection,state,expires_at,status_until,payload_limit,record_limit,index_limit,storage_limit,current_digest) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'building',?,?,?,?,?,?,?)")
                .bind(principal).bind(workspace_id).bind(&request.job_id).bind(&generation)
                .bind(&self.note_pages.id).bind(&request.header_digest).bind(&header)
                .bind(&source.snapshot_id).bind(&source.source_revision).bind(&source.scope.note_id)
                .bind(&source.scope.note_instance_id).bind(&source.source_collection)
                .bind(expires_at).bind(status_until)
                .bind(super::signed(reservation.payload_bytes)?).bind(super::signed(reservation.records)?)
                .bind(super::signed(reservation.index_entries)?).bind(super::signed(reservation.storage_charge_bytes)?)
                .bind(&request.header_digest).execute(&mut *connection).await.map_err(db_error)?;
            let row = sqlx::query(&query).bind(principal).bind(workspace_id).bind(&request.job_id)
                .fetch_one(&mut *connection).await.map_err(db_error)?;
            // Runtime eviction/expiry can race SQL awaits; rollback the logical
            // reservation if its original source lease is no longer current.
            self.note_pages.snapshot(&source.snapshot_id, workspace_id, &source.scope.note_id, principal)?;
            if intent_core::now_epoch_ms() >= u64::try_from(expires_at).map_err(|_| invalid())? {
                return Err(invalid());
            }
            status(&self.note_pages, &row).map(|state| (state, source))
        }.await;
        let (state, source) = result?;
        Self::commit_artifact_with_source_guard(source_guard, connection, source).await?;
        Ok(state)
    }
}
