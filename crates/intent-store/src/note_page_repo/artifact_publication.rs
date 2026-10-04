//! Transactional journal sealing and lease identity. Actual native index/profile
//! finalization, physical allocation and consumer reads are separate obligations.
use super::{
    artifact_lifecycle::{identifier, status, STATUS_COLUMNS},
    db_error, invalid, ArtifactJournalStatus, ArtifactSourceGrant,
};
use crate::Store;
use intent_core::{
    note_artifact::request::{ArtifactAdmit, ArtifactHeader, ArtifactSeal},
    Error, Result,
};
use sqlx::{sqlite::SqliteRow, Row, SqliteConnection};

/// Original logical admission receipt. Replaying this identity does not revive
/// its underlying lease or assert that a consumer mounted native output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactJournalLease {
    pub job_id: String,
    pub header_digest: String,
    pub final_digest: String,
    pub admission_id: String,
    pub artifact_ref: String,
    pub generation: String,
    pub expires_at: i64,
    pub status_until: i64,
}

impl Store {
    async fn artifact_publication_job(
        &self,
        source_guard: &mut SqliteConnection,
        tx: &mut SqliteConnection,
        principal: &str,
        workspace_id: &str,
        job_ref: &str,
    ) -> Result<(ArtifactJournalStatus, ArtifactSourceGrant)> {
        identifier(principal)?;
        identifier(workspace_id)?;
        let token = self.note_pages.decode(job_ref)?;
        let generation = token.2.strip_prefix("j:").ok_or_else(invalid)?;
        if token.1 != "r" || token.3 != 0 || uuid::Uuid::parse_str(generation).is_err() {
            return Err(invalid());
        }
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
        let state = status(&self.note_pages, &row)?;
        if row.try_get::<String, _>("runtime_id").map_err(db_error)? != self.note_pages.id {
            return Err(invalid());
        }
        let header: ArtifactHeader = serde_json::from_str(row.try_get("header").map_err(db_error)?)
            .map_err(|_| Error::Internal("Invalid stored artifact header".into()))?;
        let source = self
            .authorize_artifact_source_in(source_guard, workspace_id, principal, &header)
            .await?;
        self.artifact_publication_current(principal, &state, &source)?;
        Ok((state, source))
    }

    fn artifact_publication_current(
        &self,
        principal: &str,
        state: &ArtifactJournalStatus,
        source: &ArtifactSourceGrant,
    ) -> Result<()> {
        self.note_pages.snapshot(
            &source.snapshot_id,
            &source.scope.workspace_id,
            &source.scope.note_id,
            principal,
        )?;
        if state.deadline_expired()? {
            return Err(invalid());
        }
        Ok(())
    }

    /// Privately freeze a complete journal with exact accepted totals/digest.
    /// The trusted service must first finalize native/profile indexes and physical
    /// ownership. This repository operation alone does not prove either or
    /// expose a consumer-readable artifact. Its SQL finalization scans the staged
    /// records once; that construction cost is separate from bounded read cost.
    ///
    /// # Errors
    /// Rejects unauthorized/stale source, incomplete or mismatched records,
    /// invalid state transitions, and database failures.
    pub async fn seal_note_artifact_journal(
        &self,
        principal: &str,
        workspace_id: &str,
        request: &ArtifactSeal,
    ) -> Result<ArtifactJournalStatus> {
        request.validate().map_err(|_| invalid())?;
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
        let (mut state, source) = self
            .artifact_publication_job(
                &mut source_guard,
                &mut tx,
                principal,
                workspace_id,
                &request.job_ref,
            )
            .await?;
        if state.current_digest != request.final_digest
            || state.next_sequence != super::signed(request.expected_records)?
            || state.accepted_bytes != super::signed(request.expected_bytes)?
            || state.cleanup_complete
        {
            return Err(Error::InvalidParams("Artifact seal totals mismatch".into()));
        }
        match state.state.as_str() {
            "building" => {
                // The transition trigger verifies actual records/ACKs and final
                // manifest position, not only the cached aggregate counters.
                sqlx::query("UPDATE note_artifact_job SET state='sealed' WHERE generation=?")
                    .bind(&state.generation)
                    .execute(&mut *tx)
                    .await
                    .map_err(db_error)?;
                state.state = "sealed".into();
                state.private_artifact_ref = Some(
                    self.note_pages
                        .reference(&source.snapshot_id, &format!("p:{}", state.generation)),
                );
            }
            "sealed" | "admitted" => {}
            _ => return Err(invalid()),
        }
        self.artifact_publication_current(principal, &state, &source)?;
        Self::commit_artifact_with_source_guard(source_guard, tx, source).await?;
        Ok(state)
    }

    /// Atomically create one provisional lease identity, or replay its original
    /// receipt without changing release/state/capacity. Profile/native publication
    /// and physical ownership must already have been finalized by the service.
    /// This prepared journal seam does not implement the artifact read route.
    ///
    /// # Errors
    /// Rejects unauthorized/stale source, unsealed/retired generations, conflicting
    /// admission identity or final digest, and database failures.
    pub async fn admit_note_artifact_journal(
        &self,
        principal: &str,
        workspace_id: &str,
        request: &ArtifactAdmit,
    ) -> Result<ArtifactJournalLease> {
        request.validate().map_err(|_| invalid())?;
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
        let (state, source) = self
            .artifact_publication_job(
                &mut source_guard,
                &mut tx,
                principal,
                workspace_id,
                &request.job_ref,
            )
            .await?;
        if state.current_digest != request.final_digest {
            return Err(Error::InvalidParams(
                "Artifact admission digest mismatch".into(),
            ));
        }
        let query = "SELECT admission_id,lease_id,final_digest,expires_at FROM note_artifact_lease WHERE generation=?";
        let existing = sqlx::query(query)
            .bind(&state.generation)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db_error)?;
        let row = if let Some(row) = existing {
            if row.try_get::<String, _>("admission_id").map_err(db_error)? != request.admission_id
                || row.try_get::<String, _>("final_digest").map_err(db_error)?
                    != request.final_digest
            {
                return Err(Error::InvalidParams(
                    "Artifact admission identity mismatch".into(),
                ));
            }
            // A released/reclaimed or subsequently aborted lease is historical
            // identity only. Never INSERT/UPDATE it on an identical retry.
            row
        } else {
            if state.state != "sealed" || state.cleanup_complete {
                return Err(invalid());
            }
            let lease = uuid::Uuid::new_v4().simple().to_string();
            sqlx::query("INSERT INTO note_artifact_lease(generation,admission_id,lease_id,final_digest,expires_at) VALUES (?,?,?,?,?)")
                .bind(&state.generation).bind(&request.admission_id).bind(&lease)
                .bind(&request.final_digest).bind(state.expires_at).execute(&mut *tx).await.map_err(db_error)?;
            sqlx::query(query)
                .bind(&state.generation)
                .fetch_one(&mut *tx)
                .await
                .map_err(db_error)?
        };
        let receipt = self.artifact_lease_receipt(&state, &source, &row)?;
        self.artifact_publication_current(principal, &state, &source)?;
        Self::commit_artifact_with_source_guard(source_guard, tx, source).await?;
        Ok(receipt)
    }

    fn artifact_lease_receipt(
        &self,
        state: &ArtifactJournalStatus,
        source: &ArtifactSourceGrant,
        row: &SqliteRow,
    ) -> Result<ArtifactJournalLease> {
        let lease: String = row.try_get("lease_id").map_err(db_error)?;
        if uuid::Uuid::parse_str(&lease).is_err() {
            return Err(Error::Internal("Invalid stored artifact lease".into()));
        }
        Ok(ArtifactJournalLease {
            job_id: state.job_id.clone(),
            header_digest: state.header_digest.clone(),
            final_digest: row.try_get("final_digest").map_err(db_error)?,
            admission_id: row.try_get("admission_id").map_err(db_error)?,
            artifact_ref: self
                .note_pages
                .reference(&source.snapshot_id, &format!("l:{lease}")),
            generation: state.generation.clone(),
            expires_at: row.try_get("expires_at").map_err(db_error)?,
            status_until: state.status_until,
        })
    }
}
