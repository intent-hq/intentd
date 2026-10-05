//! Transactional chunk append; no public staged route is enabled by this seam.
use super::{append, db, fail, require_workspace, stream_name};
use crate::Store;
use intent_core::{
    note_mutation::{NoteMutationError, NoteOperationStatusQuery},
    note_stage::{NoteStageAppend, NoteStageHeader, NOTE_STAGE_STREAMS},
    Result,
};
use serde_json::{json, Value};
use sqlx::Row;

impl Store {
    /// Append one bounded chunk under the original operation owner and deadline.
    /// The service owns current authorization and request concurrency admission.
    /// # Errors
    /// Rejects ownership, expiry, stream conflicts or storage failure; all partial writes roll back.
    pub async fn append_note_stage(
        &self,
        principal: &str,
        request: &NoteStageAppend,
    ) -> Result<Value> {
        crate::with_write_txn_retry(|| self.append_note_stage_once(principal, request)).await
    }
    async fn append_note_stage_once(
        &self,
        principal: &str,
        request: &NoteStageAppend,
    ) -> Result<Value> {
        NoteOperationStatusQuery {
            backend_id: request.backend_id.clone(),
            workspace_id: request.workspace_id.clone(),
            note_id: request.note_id.clone(),
            note_instance_id: request.note_instance_id.clone(),
            operation_id: request.operation_id.clone(),
            header_digest: Some(request.header_digest.clone()),
            payload_digest: None,
        }
        .validate()
        .map_err(fail)?;
        if principal.is_empty() || principal.len() > 256 || principal.contains('\0') {
            return Err(fail(NoteMutationError::Invalid));
        }
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db)?;
        require_workspace(&mut tx, &request.workspace_id).await?;
        let row=sqlx::query("SELECT o.operation_key,o.method_kind,o.outcome,s.header,s.header_digest,s.phase FROM note_operation o LEFT JOIN note_stage s USING(operation_key) WHERE o.principal=? AND o.backend_id=? AND o.workspace_id=? AND o.note_id=? AND o.instance_id=? AND o.operation_id=?")
            .bind(principal).bind(&request.backend_id).bind(&request.workspace_id).bind(&request.note_id).bind(&request.note_instance_id).bind(&request.operation_id)
            .fetch_optional(&mut *tx).await.map_err(db)?.ok_or_else(||fail(NoteMutationError::Invalid))?;
        if row.get::<String, _>("method_kind") != "staged"
            || row.get::<Option<String>, _>("header_digest").as_deref()
                != Some(request.header_digest.as_str())
        {
            return Err(fail(NoteMutationError::Mismatch));
        }
        let mut state: Value = serde_json::from_str(row.get("outcome")).map_err(db)?;
        let expiry = state["expiresAt"]
            .as_str()
            .and_then(intent_core::parse_iso)
            .ok_or_else(|| fail(NoteMutationError::Invalid))?;
        if expiry
            <= intent_core::parse_iso(&intent_core::now_iso())
                .ok_or_else(|| fail(NoteMutationError::Invalid))?
        {
            return Err(fail(NoteMutationError::Expired));
        }
        if row.get::<String, _>("phase") != "staging" {
            return Err(fail(NoteMutationError::Invalid));
        }
        let header: NoteStageHeader = serde_json::from_str(row.get("header")).map_err(db)?;
        let key: String = row.get("operation_key");
        // Helper errors propagate directly. No catch/commit path can retain a
        // partial chunk, text piece or tail after SQLite statement ABORT.
        let ack = append::append(&mut tx, &key, &header, request).await?;
        if json!({"jsonrpc":"2.0","id":"\u{1}".repeat(64),"result":ack})
            .to_string()
            .len()
            > 4096
        {
            return Err(fail(NoteMutationError::Budget));
        }
        let mut summaries = Vec::with_capacity(5);
        for stream in NOTE_STAGE_STREAMS {
            let row=sqlx::query("SELECT next_sequence,last_digest FROM note_stage_stream WHERE operation_key=? AND stream=?")
                .bind(&key).bind(stream_name(stream)).fetch_one(&mut *tx).await.map_err(db)?;
            summaries.push(json!({"stream":stream,"nextSequence":row.get::<i64,_>("next_sequence"),"lastDigest":row.get::<Option<String>,_>("last_digest")}));
        }
        state["streams"] = json!(summaries);
        sqlx::query("UPDATE note_operation SET outcome=? WHERE operation_key=?")
            .bind(state.to_string())
            .bind(&key)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        if expiry
            <= intent_core::parse_iso(&intent_core::now_iso())
                .ok_or_else(|| fail(NoteMutationError::Invalid))?
        {
            return Err(fail(NoteMutationError::Expired));
        }
        tx.commit().await.map_err(db)?;
        Ok(ack)
    }
}
