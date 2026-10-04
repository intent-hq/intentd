//! Private journal operations; transport receipts and physical cleanup are
//! separate. These operations never resurrect source or extend its expiry.
use super::{db_error, invalid};
use crate::Store;
use intent_core::{Error, Result};
use sqlx::{sqlite::SqliteRow, Row};

/// Bounded internal status without record bodies, native output or source text.
#[derive(Clone, Debug)]
pub struct ArtifactJournalStatus {
    pub job_id: String,
    pub job_ref: String,
    pub generation: String,
    pub header_digest: String,
    pub state: String,
    pub next_sequence: i64,
    pub accepted_bytes: i64,
    pub current_digest: String,
    pub expires_at: i64,
    pub status_until: i64,
    pub cleanup_complete: bool,
    pub private_artifact_ref: Option<String>,
}

pub(super) const STATUS_COLUMNS: &str = "job_id,generation,source_snapshot,header_digest,state,next_sequence,accepted_bytes,current_digest,expires_at,status_until,cleanup_complete";

pub(super) fn status(runtime: &super::Runtime, row: &SqliteRow) -> Result<ArtifactJournalStatus> {
    let snapshot: String = row.try_get("source_snapshot").map_err(db_error)?;
    let generation: String = row.try_get("generation").map_err(db_error)?;
    if uuid::Uuid::parse_str(&snapshot).is_err() || uuid::Uuid::parse_str(&generation).is_err() {
        return Err(Error::Internal("Invalid stored artifact identity".into()));
    }
    let state: String = row.try_get("state").map_err(db_error)?;
    let private_artifact_ref = matches!(state.as_str(), "sealed" | "admitted")
        .then(|| runtime.reference(&snapshot, &format!("p:{generation}")));
    Ok(ArtifactJournalStatus {
        job_id: row.try_get("job_id").map_err(db_error)?,
        job_ref: runtime.reference(&snapshot, &format!("j:{generation}")),
        generation: row.try_get("generation").map_err(db_error)?,
        header_digest: row.try_get("header_digest").map_err(db_error)?,
        state,
        next_sequence: row.try_get("next_sequence").map_err(db_error)?,
        accepted_bytes: row.try_get("accepted_bytes").map_err(db_error)?,
        current_digest: row.try_get("current_digest").map_err(db_error)?,
        expires_at: row.try_get("expires_at").map_err(db_error)?,
        status_until: row.try_get("status_until").map_err(db_error)?,
        cleanup_complete: row.try_get("cleanup_complete").map_err(db_error)?,
        private_artifact_ref,
    })
}

pub(super) fn identifier(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 256 || value.contains('\0') {
        return Err(invalid());
    }
    Ok(())
}

impl Store {
    /// Look up a lost acknowledgement using the captured connection principal.
    /// A retained terminal row is returned unchanged; no read grant is minted.
    ///
    /// # Errors
    /// Rejects invalid identifiers, mismatched replay identity, corrupt stored
    /// identity, and database failures.
    pub async fn note_artifact_journal_status(
        &self,
        principal: &str,
        workspace_id: &str,
        job_id: &str,
        header_digest: &str,
    ) -> Result<Option<ArtifactJournalStatus>> {
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
        let query = format!("SELECT {STATUS_COLUMNS} FROM note_artifact_job WHERE principal=? AND workspace_id=? AND job_id=?");
        let row = sqlx::query(&query)
            .bind(principal)
            .bind(workspace_id)
            .bind(job_id)
            .fetch_optional(self.read_pool())
            .await
            .map_err(db_error)?;
        let Some(row) = row else { return Ok(None) };
        if row
            .try_get::<String, _>("header_digest")
            .map_err(db_error)?
            != header_digest
        {
            return Err(Error::InvalidParams("Artifact identity mismatch".into()));
        }
        status(&self.note_pages, &row).map(Some)
    }

    /// Retire private staging or an unadopted lease under its original signed job
    /// handle. Cleanup remains authorized after source expiry/restart, while
    /// current source reads and publication do not. Physical charges stay intact.
    ///
    /// # Errors
    /// Rejects invalid signed handles, missing scoped jobs, corrupt stored
    /// identity, and database or transaction failures.
    pub async fn abort_note_artifact_journal(
        &self,
        principal: &str,
        workspace_id: &str,
        job_ref: &str,
    ) -> Result<ArtifactJournalStatus> {
        identifier(principal)?;
        identifier(workspace_id)?;
        let token = self.note_pages.decode(job_ref)?;
        let generation = token.2.strip_prefix("j:").ok_or_else(invalid)?;
        if token.1 != "r" || token.3 != 0 || uuid::Uuid::parse_str(generation).is_err() {
            return Err(invalid());
        }
        // The guard rolls back even if cancellation interrupts BEGIN or a later
        // SQL await; a raw BEGIN can leave the sole pooled writer in a transaction.
        let mut connection = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_error)?;
        let query = format!("SELECT {STATUS_COLUMNS} FROM note_artifact_job WHERE generation=? AND principal=? AND workspace_id=? AND source_snapshot=?");
        let row = sqlx::query(&query)
            .bind(generation)
            .bind(principal)
            .bind(workspace_id)
            .bind(&token.0)
            .fetch_optional(&mut *connection)
            .await
            .map_err(db_error)?
            .ok_or_else(|| Error::NotFound("Artifact job not found".into()))?;
        let mut current = status(&self.note_pages, &row)?;
        if matches!(current.state.as_str(), "building" | "sealed" | "admitted") {
            sqlx::query("UPDATE note_artifact_job SET state='aborted' WHERE generation=?")
                .bind(generation)
                .execute(&mut *connection)
                .await
                .map_err(db_error)?;
            current.state = "aborted".into();
            current.private_artifact_ref = None;
        }
        // The state trigger atomically retires any provisional lease. This
        // same transaction retains counters and immutable replay identity.
        connection.commit().await.map_err(db_error)?;
        Ok(current)
    }
}

impl Store {
    /// Retire exactly the authenticated consumer lease. Ordinary release retains
    /// the admitted job and original receipt, including after expiry or restart.
    /// Physical reclamation is a separate operation with separate accounting.
    ///
    /// # Errors
    /// Rejects invalid signed handles, missing scoped leases, and database
    /// failures. Releasing an already released matching lease succeeds.
    pub async fn release_note_artifact_lease(
        &self,
        principal: &str,
        workspace_id: &str,
        artifact_ref: &str,
    ) -> Result<()> {
        identifier(principal)?;
        identifier(workspace_id)?;
        let token = self.note_pages.decode(artifact_ref)?;
        let lease = token.2.strip_prefix("l:").ok_or_else(invalid)?;
        if token.1 != "r" || token.3 != 0 || uuid::Uuid::parse_str(lease).is_err() {
            return Err(invalid());
        }
        // One atomic UPDATE both authorizes the captured principal/scope and
        // retires only this lease. Repeated release matches the retained row.
        let changed = sqlx::query("UPDATE note_artifact_lease SET released=1 WHERE lease_id=? AND generation IN (SELECT generation FROM note_artifact_job WHERE principal=? AND workspace_id=? AND source_snapshot=?)")
            .bind(lease).bind(principal).bind(workspace_id).bind(&token.0)
            .execute(self.write_pool()).await.map_err(db_error)?;
        if changed.rows_affected() == 0 {
            return Err(Error::NotFound("Artifact lease not found".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn release_retires_only_its_scoped_lease_and_preserves_receipts() {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(&directory.path().join("artifact.sqlite"))
            .await
            .unwrap();
        for (kind, id) in [("global", ""), ("principal", "alice"), ("workspace", "ws")] {
            sqlx::query("INSERT INTO note_artifact_capacity(scope_kind,scope_id,payload_limit,record_limit,index_limit,storage_limit,job_limit) VALUES (?,?,100,1,1,4096,1)")
                .bind(kind).bind(id).execute(store.write_pool()).await.unwrap();
        }
        let digest = "0".repeat(64);
        let generation = "00000000000000000000000000000001";
        let snapshot = "00000000000000000000000000000002";
        let lease = "00000000000000000000000000000003";
        sqlx::query("INSERT INTO note_artifact_job(principal,workspace_id,job_id,generation,runtime_id,header_digest,header,source_snapshot,source_revision,note_id,note_instance_id,source_collection,state,expires_at,status_until,payload_limit,record_limit,index_limit,storage_limit,current_digest) VALUES ('alice','ws','job',?,'runtime',?,'{}',?,'revision','note','instance','f:source','building',100,200,100,1,1,4096,?)")
            .bind(generation).bind(&digest).bind(snapshot).bind(&digest).execute(store.write_pool()).await.unwrap();
        sqlx::query("INSERT INTO note_artifact_record(generation,sequence,previous_digest,digest,record,index_charge,storage_charge,is_manifest) VALUES (?,0,?,?,'{}',1,100,1)")
            .bind(generation).bind(&digest).bind(&digest).execute(store.write_pool()).await.unwrap();
        sqlx::query("UPDATE note_artifact_job SET state='sealed' WHERE generation=?")
            .bind(generation)
            .execute(store.write_pool())
            .await
            .unwrap();
        sqlx::query("INSERT INTO note_artifact_lease(generation,admission_id,lease_id,final_digest,expires_at) VALUES (?,'admission',?,?,100)")
            .bind(generation).bind(lease).bind(&digest).execute(store.write_pool()).await.unwrap();
        let reference = store.note_pages.reference(snapshot, &format!("l:{lease}"));
        for (principal, workspace) in [("bob", "ws"), ("alice", "other")] {
            assert!(store
                .release_note_artifact_lease(principal, workspace, &reference)
                .await
                .is_err());
        }
        let wrong_kind = store.note_pages.reference(snapshot, &format!("j:{lease}"));
        assert!(store
            .release_note_artifact_lease("alice", "ws", &wrong_kind)
            .await
            .is_err());
        let wrong_snapshot = store
            .note_pages
            .reference(generation, &format!("l:{lease}"));
        assert!(store
            .release_note_artifact_lease("alice", "ws", &wrong_snapshot)
            .await
            .is_err());
        assert!(
            sqlx::query("UPDATE note_artifact_job SET cleanup_complete=1")
                .execute(store.write_pool())
                .await
                .is_err()
        );
        let before: i64 = sqlx::query_scalar("SELECT released FROM note_artifact_lease")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
        assert_eq!(before, 0);
        // Cleanup works after source expiry; it does not restore the source grant.
        for _ in 0..2 {
            store
                .release_note_artifact_lease("alice", "ws", &reference)
                .await
                .unwrap();
        }
        let row = sqlx::query("SELECT j.state,j.cleanup_complete,j.storage_charge,l.released,l.lease_id,l.admission_id FROM note_artifact_job j JOIN note_artifact_lease l USING(generation)")
            .fetch_one(store.read_pool()).await.unwrap();
        assert_eq!(row.get::<String, _>("state"), "admitted");
        assert_eq!(row.get::<i64, _>("cleanup_complete"), 0);
        assert_eq!(row.get::<i64, _>("storage_charge"), 100);
        assert_eq!(row.get::<i64, _>("released"), 1);
        assert_eq!(row.get::<String, _>("lease_id"), lease);
        assert_eq!(row.get::<String, _>("admission_id"), "admission");
        // A simulated physical-owner acknowledgement may refund the logical
        // reservation after release without changing historical admission state.
        sqlx::query("UPDATE note_artifact_job SET cleanup_complete=1")
            .execute(store.write_pool())
            .await
            .unwrap();
        let state: String = sqlx::query_scalar("SELECT state FROM note_artifact_job")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
        assert_eq!(state, "admitted");
        let reserved: i64 =
            sqlx::query_scalar("SELECT sum(storage_reserved) FROM note_artifact_capacity")
                .fetch_one(store.read_pool())
                .await
                .unwrap();
        assert_eq!(reserved, 0);
        assert!(sqlx::query("UPDATE note_artifact_lease SET released=0")
            .execute(store.write_pool())
            .await
            .is_err());
    }
}
