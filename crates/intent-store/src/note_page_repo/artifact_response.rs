//! Disabled service backing for exact prepared receipts and authorized recovery.
//! No profile, readability grant, public route or allocation authority is supplied.
use super::{
    artifact_lifecycle::{identifier, status, STATUS_COLUMNS},
    db_error, invalid,
};
use crate::{ArtifactJournalStatus, Store};
use intent_core::{
    note_artifact::{
        request::Reservation,
        response::{retention_timestamp, JobPhase, JobState, LeaseReceipt, Receipt},
    },
    Error, Result,
};
use sqlx::{sqlite::SqliteRow, Row};

const RECEIPT_COLUMNS: &str =
    "expires_at_text,payload_limit,record_limit,index_limit,storage_limit,source_snapshot";

fn unsigned(value: i64) -> Result<u64> {
    let value = u64::try_from(value).map_err(|_| invalid())?;
    if value > intent_core::note_artifact::request::SAFE_INTEGER {
        return Err(invalid());
    }
    Ok(value)
}

fn exact_deadline(row: &SqliteRow) -> Result<String> {
    let value: String = row.try_get("expires_at_text").map_err(db_error)?;
    if value.len() > 64 || intent_core::parse_iso(&value).is_none() {
        return Err(Error::Internal("Missing retained artifact deadline".into()));
    }
    Ok(value)
}

fn job_receipt(state: &ArtifactJournalStatus, row: &SqliteRow) -> Result<Receipt> {
    let phase = match state.state.as_str() {
        "building" => JobPhase::Building,
        "sealed" => JobPhase::Sealed,
        "admitted" => JobPhase::Admitted,
        "aborted" => JobPhase::Aborted,
        "expired" => JobPhase::Expired,
        _ => return Err(invalid()),
    };
    let private_artifact_ref = match phase {
        JobPhase::Sealed | JobPhase::Admitted => {
            Some(state.private_artifact_ref.clone().ok_or_else(invalid)?)
        }
        _ => None,
    };
    Ok(Receipt::Job(JobState {
        job_id: state.job_id.clone(),
        job_ref: state.job_ref.clone(),
        header_digest: state.header_digest.clone(),
        state: phase,
        next_sequence: unsigned(state.next_sequence)?,
        accepted_bytes: unsigned(state.accepted_bytes)?,
        current_digest: state.current_digest.clone(),
        expires_at: exact_deadline(row)?,
        status_until: retention_timestamp(state.status_until)?,
        reservation: Reservation {
            payload_bytes: unsigned(row.try_get("payload_limit").map_err(db_error)?)?,
            records: unsigned(row.try_get("record_limit").map_err(db_error)?)?,
            index_entries: unsigned(row.try_get("index_limit").map_err(db_error)?)?,
            storage_charge_bytes: unsigned(row.try_get("storage_limit").map_err(db_error)?)?,
        },
        private_artifact_ref,
    }))
}

impl Store {
    /// Project an original internal ACK using immutable, scoped server records.
    /// The supplied ACK must come from this Store's lifecycle operation, never
    /// request parameters. This does not register a public route.
    ///
    /// # Errors
    /// Rejects scope/identity mismatches, absent retained data or malformed fields.
    pub async fn note_artifact_job_receipt(
        &self,
        principal: &str,
        workspace_id: &str,
        ack: &ArtifactJournalStatus,
    ) -> Result<Receipt> {
        identifier(principal)?;
        identifier(workspace_id)?;
        let query = format!("SELECT {RECEIPT_COLUMNS} FROM note_artifact_job WHERE principal=? AND workspace_id=? AND job_id=? AND generation=? AND header_digest=?");
        let row = sqlx::query(&query)
            .bind(principal)
            .bind(workspace_id)
            .bind(&ack.job_id)
            .bind(&ack.generation)
            .bind(&ack.header_digest)
            .fetch_optional(self.artifact_pool()?)
            .await
            .map_err(db_error)?
            .ok_or_else(|| Error::NotFound("Artifact receipt unavailable".into()))?;
        job_receipt(ack, &row)
    }

    /// Recover bounded status under one authoritative arena read transaction.
    /// The service must capture membership/principal before calling. Historical
    /// admission receipts do not assert a live lease; reads remain separately
    /// authorized. Missing history never asserts rollback or reusable identity.
    ///
    /// # Errors
    /// Rejects malformed identity, mismatched digest and corrupt retained data.
    pub async fn recover_note_artifact_receipt(
        &self,
        principal: &str,
        workspace_id: &str,
        job_id: &str,
        header_digest: &str,
    ) -> Result<Receipt> {
        for value in [principal, workspace_id, job_id] {
            identifier(value)?;
        }
        if header_digest.len() != 64
            || !header_digest
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(invalid());
        }
        let mut tx = self.artifact_pool()?.begin().await.map_err(db_error)?;
        let query = format!("SELECT {STATUS_COLUMNS},{RECEIPT_COLUMNS} FROM note_artifact_job WHERE principal=? AND workspace_id=? AND job_id=?");
        let row = sqlx::query(&query)
            .bind(principal)
            .bind(workspace_id)
            .bind(job_id)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db_error)?;
        let Some(row) = row else {
            tx.commit().await.map_err(db_error)?;
            return Ok(Receipt::Unknown {
                job_id: job_id.into(),
                header_digest: header_digest.into(),
            });
        };
        let mut state = status(&self.note_pages, &row)?;
        if state.header_digest != header_digest {
            return Err(Error::InvalidParams("Artifact identity mismatch".into()));
        }
        let deadline = exact_deadline(&row)?;
        let now = intent_core::parse_iso(&intent_core::now_iso()).ok_or_else(invalid)?;
        if matches!(state.state.as_str(), "building" | "sealed" | "admitted")
            && intent_core::parse_iso(&deadline).ok_or_else(invalid)? <= now
        {
            // Effective expiry is authoritative even before bounded maintenance
            // persists it. Never conceal terminal authority behind an old lease.
            state.state = "expired".into();
            state.private_artifact_ref = None;
        }
        let receipt = if state.state == "admitted" {
            let lease = sqlx::query("SELECT admission_id,lease_id,final_digest FROM note_artifact_lease WHERE generation=?")
                .bind(&state.generation).fetch_optional(&mut *tx).await.map_err(db_error)?
                .ok_or_else(|| Error::Internal("Missing original artifact admission receipt".into()))?;
            let lease_id: String = lease.try_get("lease_id").map_err(db_error)?;
            if uuid::Uuid::parse_str(&lease_id).is_err() {
                return Err(invalid());
            }
            let snapshot: String = row.try_get("source_snapshot").map_err(db_error)?;
            Receipt::Lease(LeaseReceipt {
                job_id: state.job_id.clone(),
                header_digest: state.header_digest.clone(),
                final_digest: lease.try_get("final_digest").map_err(db_error)?,
                admission_id: lease.try_get("admission_id").map_err(db_error)?,
                artifact_ref: self
                    .note_pages
                    .reference(&snapshot, &format!("l:{lease_id}")),
                generation: state.generation.clone(),
                expires_at: deadline,
                status_until: retention_timestamp(state.status_until)?,
            })
        } else {
            job_receipt(&state, &row)?
        };
        // Retention is a minimum, not a wall-clock instruction to conceal rows.
        // No mutation, source pin, reservation or grant is created by recovery.
        tx.commit().await.map_err(db_error)?;
        Ok(receipt)
    }
}
