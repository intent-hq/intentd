//! Finite background retirement and payload removal after receipt retention.
//! Neither operation refunds reservations or runs as part of a viewport read.
use super::{artifact_lifecycle::identifier, db_error, invalid};
use crate::Store;
use intent_core::{Error, Result};
use sqlx::Row;

const MAX_RETIRE_BATCH: u32 = 128;
const MAX_PURGE_BATCH: u32 = 128;

/// Record-removal progress only, never a physical reclamation receipt.
#[derive(Debug, PartialEq, Eq)]
pub struct ArtifactJournalPurge {
    pub deleted_records: u32,
    pub more_records: bool,
}

impl Store {
    /// Remove at most `max_records` retired payloads and their append ACKs after
    /// the advertised receipt-retention deadline. The job identity/status, lease
    /// receipt, counters and every reservation remain intact. This is an internal
    /// cleanup step, not an RPC or permission to refund disk/index/constructor
    /// capacity. Arena file high-water remains charged despite reusable pages.
    /// The sole arena connection serializes this with pending record I/O.
    ///
    /// # Errors
    /// Rejects invalid/substituted handles, nonterminal jobs, unelapsed retention,
    /// zero or more than 128 requested records, and database failures.
    pub async fn purge_note_artifact_journal_records(
        &self,
        principal: &str,
        workspace_id: &str,
        job_ref: &str,
        max_records: u32,
    ) -> Result<ArtifactJournalPurge> {
        identifier(principal)?;
        identifier(workspace_id)?;
        if !(1..=MAX_PURGE_BATCH).contains(&max_records) {
            return Err(invalid());
        }
        let token = self.note_pages.decode(job_ref)?;
        let generation = token.2.strip_prefix("j:").ok_or_else(invalid)?;
        if token.1 != "r" || token.3 != 0 || uuid::Uuid::parse_str(generation).is_err() {
            return Err(invalid());
        }
        let mut tx = self
            .artifact_pool()?
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_error)?;
        let row = sqlx::query("SELECT state,status_until FROM note_artifact_job WHERE generation=? AND principal=? AND workspace_id=? AND source_snapshot=?")
            .bind(generation).bind(principal).bind(workspace_id).bind(&token.0)
            .fetch_optional(&mut *tx).await.map_err(db_error)?
            .ok_or_else(|| Error::NotFound("Artifact job not found".into()))?;
        let state: &str = row.try_get("state").map_err(db_error)?;
        let status_until: i64 = row.try_get("status_until").map_err(db_error)?;
        if !matches!(state, "aborted" | "expired")
            || status_until > i64::try_from(intent_core::now_epoch_ms()).map_err(|_| invalid())?
        {
            return Err(invalid());
        }
        // Exact generation/sequence index, bounded before mutation. Cascaded
        // ACK deletion uses the same primary key; no payload enters this reply.
        let deleted = sqlx::query("DELETE FROM note_artifact_record WHERE generation=? AND sequence IN (SELECT sequence FROM note_artifact_record WHERE generation=? ORDER BY sequence LIMIT ?)")
            .bind(generation).bind(generation).bind(max_records).execute(&mut *tx)
            .await.map_err(db_error)?.rows_affected();
        let more_records: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM note_artifact_record WHERE generation=?)",
        )
        .bind(generation)
        .fetch_one(&mut *tx)
        .await
        .map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        Ok(ArtifactJournalPurge {
            deleted_records: u32::try_from(deleted).map_err(|_| invalid())?,
            more_records,
        })
    }

    /// Retire at most `max_jobs` elapsed journals using the expiry index. The
    /// trusted background owner may repeat batches; this is not a wire method.
    /// Lease revocation commits with each state transition. Retained receipts,
    /// records and all storage reservations survive pending physical cleanup.
    ///
    /// # Errors
    /// Rejects zero or more than 128 requested jobs, and database failures.
    pub async fn expire_note_artifact_journals(&self, max_jobs: u32) -> Result<Vec<String>> {
        if !(1..=MAX_RETIRE_BATCH).contains(&max_jobs) {
            return Err(invalid());
        }
        let now = i64::try_from(intent_core::now_epoch_ms()).map_err(|_| invalid())?;
        let mut tx = self
            .artifact_pool()?
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_error)?;
        let generations = sqlx::query_scalar::<_, String>(
            "UPDATE note_artifact_job SET state='expired' WHERE generation IN \
             (SELECT generation FROM note_artifact_job WHERE state IN ('building','sealed','admitted') \
             AND expires_at<=? ORDER BY expires_at,generation LIMIT ?) RETURNING generation",
        ).bind(now).bind(max_jobs).fetch_all(&mut *tx).await.map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        Ok(generations)
    }
}
