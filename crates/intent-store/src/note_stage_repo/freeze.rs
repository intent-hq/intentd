//! Atomic transition from accepted chunks to a validated immutable view.
//! Projection rows are structural metadata, never authority to serialize a new
//! editor schema or manufacture persisted markers. Output adapters must validate
//! their node/attribute policies before using a descriptor.
use super::{db, fail, require_workspace, seal};
use crate::Store;
use intent_core::{
    note_mutation::NoteMutationError,
    note_stage::{NoteStageHeader, NoteStageSeal},
    Error, Result,
};
use serde_json::{json, Value};
use sqlx::Row;

fn live_deadline(state: &Value) -> Result<()> {
    let deadline = state["expiresAt"]
        .as_str()
        .and_then(intent_core::parse_iso)
        .ok_or_else(|| fail(NoteMutationError::Invalid))?;
    let now = intent_core::parse_iso(&intent_core::now_iso())
        .ok_or_else(|| fail(NoteMutationError::Invalid))?;
    if deadline <= now {
        return Err(fail(NoteMutationError::Expired));
    }
    Ok(())
}

impl Store {
    /// Freeze accepted chunks into an immutable operation-owned source view.
    /// Services supply current authorization and bounded request admission.
    /// No live note, version, comment or receipt effects are mutated here.
    /// # Errors
    /// Rejects changed identity/manifest, malformed references, expired or closed
    /// staging and storage failures. Every failed validation drops the writer.
    pub async fn seal_note_stage(&self, principal: &str, request: &NoteStageSeal) -> Result<Value> {
        request.validate().map_err(fail)?;
        if principal.is_empty() || principal.len() > 256 || principal.contains('\0') {
            return Err(fail(NoteMutationError::Invalid));
        }
        crate::with_write_txn_retry(|| self.seal_note_stage_once(principal, request)).await
    }

    async fn seal_note_stage_once(
        &self,
        principal: &str,
        request: &NoteStageSeal,
    ) -> Result<Value> {
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db)?;
        require_workspace(&mut tx, &request.workspace_id).await?;
        let row=sqlx::query("SELECT o.operation_key,o.method_kind,o.outcome,o.retain_until,s.header_digest,s.header,s.phase,s.root_key,s.payload_digest,s.manifest FROM note_operation o LEFT JOIN note_stage s USING(operation_key) WHERE o.principal=? AND o.backend_id=? AND o.workspace_id=? AND o.note_id=? AND o.instance_id=? AND o.operation_id=?")
            .bind(principal).bind(&request.backend_id).bind(&request.workspace_id).bind(&request.note_id).bind(&request.note_instance_id).bind(&request.operation_id)
            .fetch_optional(&mut *tx).await.map_err(db)?.ok_or_else(||fail(NoteMutationError::Invalid))?;
        if row.get::<String, _>("method_kind") != "staged"
            || row.get::<Option<String>, _>("header_digest").as_deref()
                != Some(request.header_digest.as_str())
        {
            return Err(fail(NoteMutationError::Mismatch));
        }
        let mut state: Value = serde_json::from_str(row.get("outcome")).map_err(db)?;
        let manifest = serde_json::to_string(&request.manifest).map_err(db)?;
        let phase: String = row.get("phase");
        let key: String = row.get("operation_key");
        if let Some(accepted) = row.get::<Option<String>, _>("payload_digest") {
            if accepted != request.payload_digest
                || row.get::<Option<String>, _>("manifest").as_deref() != Some(manifest.as_str())
            {
                return Err(fail(NoteMutationError::Mismatch));
            }
            // Lookup/replay precedes any comparison with the current source.
            // A remote write cannot rewrite the captured base or sealed view.
            if phase == "committed" {
                let now = intent_core::parse_iso(&intent_core::now_iso())
                    .ok_or_else(|| fail(NoteMutationError::Invalid))?;
                if row.get::<i64, _>("retain_until") <= now.unix_timestamp() {
                    return Err(fail(NoteMutationError::Expired));
                }
            } else if phase == "sealed" {
                live_deadline(&state)?;
            } else {
                return Err(fail(NoteMutationError::Expired));
            }
            return Ok(state);
        }
        if phase != "staging" {
            return Err(fail(NoteMutationError::Expired));
        }
        live_deadline(&state)?;
        let header: NoteStageHeader = serde_json::from_str(row.get("header")).map_err(db)?;
        let root: String = row.get("root_key");
        let view = seal::prepare_frozen_view(&mut tx, &key, &header, request, &root).await?;
        seal::validate_live_descriptors(&mut tx, &key, &view).await?;
        // Marker occurrence provenance needs its actual canonical adapter. Until
        // that adapter is registered this case fails closed, never treats an
        // arbitrary canonicalId or native range as persisted source authority.
        let marker:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM note_stage_validation WHERE operation_key=? AND kind='live' AND json_extract(value,'$.role')='marker-occurrence')")
            .bind(&key).fetch_one(&mut *tx).await.map_err(db)?;
        if marker {
            return Err(Error::Unsupported("staged canonical marker adapter".into()));
        }
        if header.output == intent_core::note_stage::NoteStageOutput::Search
            && header.query.as_ref().is_some_and(|query| {
                query.mode == intent_core::note_stage::NoteStageSearchMode::Source
            })
        {
            super::search_ranges::normalize(&mut tx, &key, &header, &view).await?;
        }
        state["phase"] = json!("sealed");
        state["payloadDigest"] = json!(request.payload_digest);
        state["viewLength"] = json!(view.length);
        if json!({"jsonrpc":"2.0","id":"\u{1}".repeat(64),"result":state})
            .to_string()
            .len()
            > 4096
        {
            return Err(fail(NoteMutationError::Budget));
        }
        sqlx::query("UPDATE note_stage SET phase='sealed',manifest=?,payload_digest=?,view_length=?,view_id=? WHERE operation_key=? AND phase='staging'")
            .bind(manifest).bind(&request.payload_digest).bind(i64::try_from(view.length).map_err(db)?).bind(view.view_id).bind(&key).execute(&mut *tx).await.map_err(db)?;
        sqlx::query("UPDATE note_operation SET payload_digest=?,outcome=? WHERE operation_key=?")
            .bind(&request.payload_digest)
            .bind(state.to_string())
            .bind(&key)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        // All awaited validation/publication is covered by the original deadline;
        // never start a fresh lease at seal or hide an error behind phase fallback.
        live_deadline(&state)?;
        tx.commit().await.map_err(db)?;
        Ok(state)
    }
}
