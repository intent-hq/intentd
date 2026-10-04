//! Finite background retirement. This never refunds physical or logical storage
//! charges, deletes accepted records, or runs as part of a viewport read.
use super::{db_error, invalid};
use crate::Store;
use intent_core::Result;

const MAX_RETIRE_BATCH: u32 = 128;

impl Store {
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
            .write_pool()
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
