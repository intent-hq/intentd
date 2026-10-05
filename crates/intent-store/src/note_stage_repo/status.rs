//! Fixed-size staged status and cancellation. Cancelling changes admission only;
//! external pieces are reclaimed by separate bounded cleanup, never cascaded in
//! the caller's request. Services retain current authorization responsibility.
use super::{db, fail, require_workspace};
use crate::Store;
use intent_core::{
    note_mutation::{NoteMutationError, NoteOperationStatusQuery},
    note_stage::NoteStageCancel,
    Result,
};
use serde_json::{json, Value};
use sqlx::{Row, SqliteConnection};

fn unknown(request: &NoteOperationStatusQuery) -> Value {
    let mut value = json!({"kind":"noteOperationStatus","outcome":"unknown","scope":request.scope(),"operationId":request.operation_id,"headerDigest":request.header_digest});
    if let Some(digest) = &request.payload_digest {
        value["payloadDigest"] = json!(digest);
    }
    value
}

async fn lookup(
    conn: &mut SqliteConnection,
    principal: &str,
    request: &NoteOperationStatusQuery,
) -> Result<Option<(String, Value)>> {
    request.validate().map_err(fail)?;
    let digest = request
        .header_digest
        .as_deref()
        .ok_or_else(|| fail(NoteMutationError::Invalid))?;
    if principal.is_empty() || principal.len() > 256 || principal.contains('\0') {
        return Err(fail(NoteMutationError::Invalid));
    }
    require_workspace(conn, &request.workspace_id).await?;
    let row=sqlx::query("SELECT o.operation_key,o.method_kind,o.retain_until,o.outcome,s.header_digest,s.payload_digest FROM note_operation o LEFT JOIN note_stage s USING(operation_key) WHERE o.principal=? AND o.backend_id=? AND o.workspace_id=? AND o.note_id=? AND o.instance_id=? AND o.operation_id=?")
        .bind(principal).bind(&request.backend_id).bind(&request.workspace_id).bind(&request.note_id).bind(&request.note_instance_id).bind(&request.operation_id)
        .fetch_optional(&mut *conn).await.map_err(db)?;
    let Some(row) = row else {
        return Ok(None);
    };
    if row.get::<String, _>("method_kind") != "staged"
        || row.get::<Option<String>, _>("header_digest").as_deref() != Some(digest)
    {
        return Err(fail(NoteMutationError::Mismatch));
    }
    if let Some(expected) = &request.payload_digest {
        if row.get::<Option<String>, _>("payload_digest").as_deref() != Some(expected) {
            return Err(fail(NoteMutationError::Mismatch));
        }
    }
    let now = intent_core::parse_iso(&intent_core::now_iso())
        .ok_or_else(|| fail(NoteMutationError::Invalid))?;
    if row.get::<i64, _>("retain_until") <= now.unix_timestamp() {
        return Ok(None);
    }
    let mut state: Value = serde_json::from_str(row.get("outcome")).map_err(db)?;
    if state["kind"] == "noteStageState"
        && matches!(state["phase"].as_str(), Some("staging" | "sealed"))
    {
        let deadline = state["expiresAt"]
            .as_str()
            .and_then(intent_core::parse_iso)
            .ok_or_else(|| fail(NoteMutationError::Invalid))?;
        if deadline <= now {
            state["phase"] = json!("expired");
        }
    }
    Ok(Some((row.get("operation_key"), state)))
}

impl Store {
    /// Look up the original staged owner without hydrating a current Note.
    /// # Errors
    /// Rejects malformed/mismatched identity, retired workspace or storage error.
    pub async fn note_stage_status(
        &self,
        principal: &str,
        request: &NoteOperationStatusQuery,
    ) -> Result<Value> {
        let mut tx = self.read_pool().begin().await.map_err(db)?;
        Ok(lookup(&mut tx, principal, request)
            .await?
            .map_or_else(|| unknown(request), |(_, state)| state))
    }

    /// Cancel only an uncommitted operation. Exact committed outcomes are retained.
    /// # Errors
    /// Rejects malformed/rebound identity or storage error; cancellation is atomic.
    pub async fn cancel_note_stage(
        &self,
        principal: &str,
        request: &NoteStageCancel,
    ) -> Result<Value> {
        request.validate().map_err(fail)?;
        crate::with_write_txn_retry(|| self.cancel_note_stage_once(principal, request)).await
    }

    async fn cancel_note_stage_once(
        &self,
        principal: &str,
        request: &NoteStageCancel,
    ) -> Result<Value> {
        let query = NoteOperationStatusQuery {
            backend_id: request.backend_id.clone(),
            workspace_id: request.workspace_id.clone(),
            note_id: request.note_id.clone(),
            note_instance_id: request.note_instance_id.clone(),
            operation_id: request.operation_id.clone(),
            header_digest: Some(request.header_digest.clone()),
            payload_digest: None,
        };
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db)?;
        let Some((key, mut state)) = lookup(&mut tx, principal, &query).await? else {
            return Ok(unknown(&query));
        };
        if state["kind"] != "noteStageState" {
            tx.commit().await.map_err(db)?;
            return Ok(state);
        }
        let phase = match state["phase"].as_str() {
            Some("staging" | "sealed" | "cancelled") => "cancelled",
            Some("expired") => "expired",
            _ => return Err(fail(NoteMutationError::Invalid)),
        };
        state["phase"] = json!(phase);
        sqlx::query("UPDATE note_stage SET phase=? WHERE operation_key=?")
            .bind(phase)
            .bind(&key)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::query("UPDATE note_operation SET outcome=? WHERE operation_key=?")
            .bind(state.to_string())
            .bind(&key)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(state)
    }
}
