//! Internal immutable staged admission; public lifecycle routes remain gated.
use crate::Store;
use intent_core::{
    note_mutation::NoteMutationError,
    note_stage::{NoteStageBegin, NoteStageTail, NOTE_STAGE_STREAMS},
    Error, Result,
};
use serde_json::{json, Value};
use sqlx::Row;
mod append;
mod freeze;
mod output;
mod read;
mod reclaim;
pub use reclaim::NoteOperationReclaimStats;
mod seal;
mod search_detail;
mod search_output;
mod search_ranges;
mod source;
mod status;
pub(crate) mod view_read;
mod write;
fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("note stage storage: {error}"))
}
fn fail(error: NoteMutationError) -> Error {
    Error::NoteMutation(error)
}
// Retirement is durable source admission, not merely an authorization policy.
// Check it even on exact replay: partial workspace cleanup must not accept new
// staging work or present a live-looking operation over retired source.
async fn require_workspace(conn: &mut sqlx::SqliteConnection, workspace: &str) -> Result<()> {
    let available: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workspace w WHERE w.id=? AND NOT EXISTS(SELECT 1 FROM note_annotation_workspace_retirement r WHERE r.workspace_id=w.id))")
        .bind(workspace).fetch_one(conn).await.map_err(db)?;
    if !available {
        return Err(Error::NotFound("Workspace not found".into()));
    }
    Ok(())
}
fn stream_name(stream: intent_core::note_stage::NoteStageStream) -> &'static str {
    use intent_core::note_stage::NoteStageStream;
    match stream {
        NoteStageStream::Text => "text",
        NoteStageStream::Dirty => "dirty",
        NoteStageStream::Selection => "selection",
        NoteStageStream::Mutation => "mutation",
        NoteStageStream::Live => "live",
    }
}
impl Store {
    /// Admit an immutable source root without copying the note or holding a writer across requests.
    /// The service owns current authorization.
    /// # Errors
    /// Rejects stale scope, expired admission and reused operation identity.
    pub async fn begin_note_stage(
        &self,
        principal: &str,
        request: &NoteStageBegin,
    ) -> Result<Value> {
        crate::with_write_txn_retry(|| self.begin_note_stage_once(principal, request)).await
    }
    async fn begin_note_stage_once(
        &self,
        principal: &str,
        request: &NoteStageBegin,
    ) -> Result<Value> {
        request.validate().map_err(fail)?;
        if principal.is_empty() || principal.len() > 256 || principal.contains('\0') {
            return Err(fail(NoteMutationError::Invalid));
        }
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db)?;
        require_workspace(&mut tx, &request.workspace_id).await?;
        let prior=sqlx::query("SELECT o.operation_key,o.method_kind,o.outcome,o.retain_until,s.header_digest FROM note_operation o LEFT JOIN note_stage s USING(operation_key) WHERE o.principal=? AND o.backend_id=? AND o.workspace_id=? AND o.note_id=? AND o.instance_id=? AND o.operation_id=?")
            .bind(principal).bind(&request.backend_id).bind(&request.workspace_id).bind(&request.note_id).bind(&request.note_instance_id).bind(&request.operation_id)
            .fetch_optional(&mut *tx).await.map_err(db)?;
        if let Some(prior) = prior {
            if prior.get::<i64, _>("retain_until")
                <= intent_core::parse_iso(&intent_core::now_iso())
                    .ok_or_else(|| fail(NoteMutationError::Invalid))?
                    .unix_timestamp()
            {
                return Err(fail(NoteMutationError::Expired));
            }
            if prior.get::<String, _>("method_kind") != "staged"
                || prior.get::<Option<String>, _>("header_digest").as_deref()
                    != Some(request.header_digest.as_str())
            {
                return Err(fail(NoteMutationError::Mismatch));
            }
            let mut state: Value = serde_json::from_str(prior.get("outcome")).map_err(db)?;
            if state["kind"] == "noteStageState"
                && matches!(state["phase"].as_str(), Some("staging" | "sealed"))
                && intent_core::parse_iso(
                    state["expiresAt"]
                        .as_str()
                        .ok_or_else(|| fail(NoteMutationError::Invalid))?,
                )
                .ok_or_else(|| fail(NoteMutationError::Invalid))?
                    <= intent_core::parse_iso(&intent_core::now_iso())
                        .ok_or_else(|| fail(NoteMutationError::Invalid))?
            {
                state["phase"] = json!("expired");
            }
            tx.commit().await.map_err(db)?;
            return Ok(state);
        }
        request
            .validate_new_admission(
                intent_core::parse_iso(&intent_core::now_iso())
                    .ok_or_else(|| fail(NoteMutationError::Invalid))?,
            )
            .map_err(fail)?;
        let root =
            source::pin_base(&mut tx, &request.scope(), &request.header.base_revision).await?;
        let summaries: Vec<Value> = NOTE_STAGE_STREAMS
            .iter()
            .map(|stream| json!({"stream":stream,"nextSequence":0,"lastDigest":null}))
            .collect();
        let state = json!({"kind":"noteStageState","scope":request.scope(),"operationId":request.operation_id,"headerDigest":request.header_digest,
            "phase":"staging","baseRevision":request.header.base_revision,"expiresAt":request.expires_at,"streams":summaries});
        // Admission reserves the eventual fixed-size stream summaries too.
        let mut largest = state.clone();
        largest["payloadDigest"] = json!("f".repeat(64));
        largest["viewLength"] = json!(9_007_199_254_740_991u64);
        for stream in largest["streams"]
            .as_array_mut()
            .ok_or_else(|| fail(NoteMutationError::Invalid))?
        {
            stream["nextSequence"] = json!(9_007_199_254_740_991u64);
            stream["lastDigest"] = json!("f".repeat(64));
        }
        if largest.to_string().len() > 3584 {
            return Err(fail(NoteMutationError::Budget));
        }
        let deadline = intent_core::parse_iso(&request.expires_at)
            .ok_or_else(|| fail(NoteMutationError::Invalid))?;
        let operation_key = uuid::Uuid::new_v4().to_string();
        sqlx::query("INSERT INTO note_operation(operation_key,principal,backend_id,workspace_id,note_id,instance_id,operation_id,payload_digest,admission_expires,retain_until,outcome,method_kind) VALUES(?,?,?,?,?,?,?,?,?,?,?,'staged')")
            .bind(&operation_key).bind(principal).bind(&request.backend_id).bind(&request.workspace_id).bind(&request.note_id).bind(&request.note_instance_id).bind(&request.operation_id)
            .bind(&request.header_digest).bind(deadline.unix_timestamp()).bind(deadline.unix_timestamp()+7*86400+1).bind(state.to_string()).execute(&mut *tx).await.map_err(db)?;
        sqlx::query("INSERT INTO note_stage(operation_key,root_key,header_digest,header,base_revision,phase) VALUES(?,?,?,?,?,'staging')")
            .bind(&operation_key).bind(&root.key).bind(&request.header_digest).bind(serde_json::to_string(&request.header).map_err(db)?).bind(&request.header.base_revision).execute(&mut *tx).await.map_err(db)?;
        let tail = serde_json::to_string(&NoteStageTail::default()).map_err(db)?;
        for stream in NOTE_STAGE_STREAMS {
            sqlx::query("INSERT INTO note_stage_stream(operation_key,stream,tail) VALUES(?,?,?)")
                .bind(&operation_key)
                .bind(stream_name(stream))
                .bind(&tail)
                .execute(&mut *tx)
                .await
                .map_err(db)?;
        }
        if deadline
            <= intent_core::parse_iso(&intent_core::now_iso())
                .ok_or_else(|| fail(NoteMutationError::Invalid))?
        {
            return Err(fail(NoteMutationError::Expired));
        }
        tx.commit().await.map_err(db)?;
        Ok(state)
    }
}
