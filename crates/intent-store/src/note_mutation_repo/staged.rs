//! Staged commit admission holds the same writer through source CAS, canonical
//! mutation and receipt publication. Dropping a reservation rolls it back.
use super::{db, fail, NoteMutationWrite};
use crate::Store;
use intent_core::{
    note_mutation::{NoteApplySplices, NoteMutationError, NoteSourceHistory, NoteSplice},
    note_stage::{NoteStageAction, NoteStageCommit, NoteStageHeader, NoteStageRecord},
    Error, Note, Result,
};
use serde_json::Value;
use sqlx::{Row, Sqlite, Transaction};

pub enum StageCommitAdmission {
    Replay(Value),
    Reserved(Box<StageCommitReservation>),
}

pub struct StageCommitReservation {
    pub(super) transaction: Transaction<'static, Sqlite>,
    pub(super) identity: NoteApplySplices,
    pub(super) operation_key: String,
    pub(super) note: Note,
    pub(super) header_digest: String,
    pub(super) view_id: String,
    pub(super) view_generation: u64,
    pub(super) view_length: u64,
    pub(super) receipt_expires_at: String,
}

#[derive(Clone)]
pub(super) struct StagedWriteContext {
    pub(super) header_digest: String,
    pub(super) view_id: String,
    pub(super) view_generation: u64,
    pub(super) mutation_present: bool,
    // A second source ledger begins at the newest user group's original input.
    // Canonical phases extend it alongside the base-to-final ledger. Earlier
    // groups remain in external retained views, not a Vec of source snapshots.
    pub(super) newest_history: Option<NoteSourceHistory>,
}

impl StageCommitReservation {
    /// Prepare exact source provenance from the sealed streams while retaining
    /// the writer reservation. The callback is the existing logical replacement
    /// guard; it sees complete replacement text, never individual upload chunks.
    /// # Errors
    /// Any malformed retained input, rejected replacement or SQL failure drops
    /// the reservation. This is document-sized write preparation, not a read RPC.
    pub async fn into_mutation(
        mut self,
        validate_replacement: fn(&str) -> Result<()>,
    ) -> Result<Box<NoteMutationWrite>> {
        let mut history = NoteSourceHistory::new(self.note.content.clone());
        let mut newest = None;
        replay_stream(
            &mut self.transaction,
            &self.operation_key,
            "dirty",
            &mut history,
            &mut newest,
            validate_replacement,
        )
        .await?;
        // Validate the replay against the exact external sealed dirty view.
        // Only one bounded piece is loaded in addition to the writer's source.
        let mut position = 0;
        let mut byte = 0;
        while position < self.view_length {
            let (next, text) = crate::note_stage_repo::view_read::read_piece(
                &mut self.transaction,
                &self.operation_key,
                self.view_generation,
                self.view_length,
                position,
                16_384,
            )
            .await?;
            if next <= position || history.source().get(byte..byte + text.len()) != Some(&text) {
                return Err(fail(NoteMutationError::Mismatch));
            }
            position = next;
            byte += text.len();
        }
        if byte != history.source().len() {
            return Err(fail(NoteMutationError::Mismatch));
        }
        let mutation_present = replay_stream(
            &mut self.transaction,
            &self.operation_key,
            "mutation",
            &mut history,
            &mut newest,
            validate_replacement,
        )
        .await?;
        let persisted_source = self.note.content.clone();
        let mut writer = Box::new(NoteMutationWrite {
            transaction: self.transaction,
            request: self.identity,
            operation_key: self.operation_key,
            note: self.note,
            history,
            persisted_source,
            receipt_expires_at: self.receipt_expires_at,
            effects: Vec::new(),
            converted_count: 0,
            persisted_phases: 0,
            conversion: None,
            staged: Some(StagedWriteContext {
                header_digest: self.header_digest,
                view_id: self.view_id,
                view_generation: self.view_generation,
                mutation_present,
                newest_history: newest,
            }),
        });
        super::copy_source(
            &mut writer.transaction,
            &writer.operation_key,
            "base",
            &writer.request,
        )
        .await?;
        Ok(writer)
    }
}

async fn replacement_text(
    conn: &mut sqlx::SqliteConnection,
    operation: &str,
    reference: &intent_core::note_stage::NoteStageTextReference,
) -> Result<String> {
    use sha2::{Digest, Sha256};
    let mut source = String::new();
    let mut offset = 0_i64;
    while u64::try_from(offset).map_err(db)? < reference.length {
        let (end, text): (i64, String) = sqlx::query_as("SELECT end,text FROM note_stage_text_piece WHERE operation_key=? AND text_id=? AND start=?")
            .bind(operation).bind(&reference.text_id).bind(offset)
            .fetch_optional(&mut *conn).await.map_err(db)?
            .ok_or_else(|| fail(NoteMutationError::Invalid))?;
        if end <= offset
            || u64::try_from(end).map_err(db)? > reference.length
            || text.len() > 4096
            || i64::try_from(text.encode_utf16().count()).map_err(db)? != end - offset
            || source
                .len()
                .checked_add(text.len())
                .is_none_or(|bytes| bytes as u64 > reference.utf8_bytes)
        {
            return Err(fail(NoteMutationError::Invalid));
        }
        source.push_str(&text);
        offset = end;
    }
    if source.len() as u64 != reference.utf8_bytes
        || format!("{:x}", Sha256::digest(source.as_bytes())) != reference.sha256
    {
        return Err(fail(NoteMutationError::Mismatch));
    }
    Ok(source)
}

fn apply_group(
    history: &mut NoteSourceHistory,
    newest: &mut Option<NoteSourceHistory>,
    edits: &mut Vec<NoteSplice>,
) -> Result<()> {
    if edits.is_empty() {
        return Ok(());
    }
    let mut group = NoteSourceHistory::new(history.source().to_owned());
    group.apply_phase(edits).map_err(fail)?;
    history.apply_phase(edits).map_err(fail)?;
    *newest = Some(group);
    edits.clear();
    Ok(())
}

async fn replay_stream(
    conn: &mut sqlx::SqliteConnection,
    operation: &str,
    stream: &str,
    history: &mut NoteSourceHistory,
    newest: &mut Option<NoteSourceHistory>,
    validate_replacement: fn(&str) -> Result<()>,
) -> Result<bool> {
    let mut after = (-1_i64, -1_i64);
    let mut group = None;
    let mut edits = Vec::new();
    let mut any = false;
    loop {
        let row: Option<(i64, i64, String)> = sqlx::query_as("SELECT chunk_sequence,ordinal,value FROM note_stage_record WHERE operation_key=? AND stream=? AND (chunk_sequence,ordinal)>(?,?) ORDER BY chunk_sequence,ordinal LIMIT 1")
            .bind(operation).bind(stream).bind(after.0).bind(after.1)
            .fetch_optional(&mut *conn).await.map_err(db)?;
        let Some((chunk, ordinal, raw)) = row else {
            break;
        };
        after = (chunk, ordinal);
        let NoteStageRecord::Splice {
            local_sequence,
            start,
            end,
            replacement,
            ..
        } = serde_json::from_str(&raw).map_err(db)?
        else {
            return Err(fail(NoteMutationError::Invalid));
        };
        if any && group != local_sequence {
            apply_group(history, newest, &mut edits)?;
        }
        group = local_sequence;
        any = true;
        let text = replacement_text(conn, operation, &replacement).await?;
        validate_replacement(&text)?;
        edits.push(NoteSplice { start, end, text });
    }
    apply_group(history, newest, &mut edits)?;
    Ok(any)
}

impl Store {
    /// Serialize cancellation/replay with an exact source CAS before preparing
    /// any write. Current caller authorization belongs to the service boundary.
    /// # Errors
    /// Rejects identity reuse, expired/unsealed staging, retirement, read-only
    /// operations and a changed/deleted/recreated source. No note write occurs.
    pub async fn reserve_note_stage_commit(
        &self,
        principal: &str,
        request: &NoteStageCommit,
    ) -> Result<StageCommitAdmission> {
        request.validate().map_err(fail)?;
        if principal.is_empty() || principal.len() > 256 || principal.contains('\0') {
            return Err(fail(NoteMutationError::Invalid));
        }
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db)?;
        let available: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workspace w WHERE w.id=? AND NOT EXISTS(SELECT 1 FROM note_annotation_workspace_retirement r WHERE r.workspace_id=w.id))")
            .bind(&request.workspace_id).fetch_one(&mut *tx).await.map_err(db)?;
        if !available {
            return Err(Error::NotFound("Workspace not found".into()));
        }
        let row = sqlx::query("SELECT o.operation_key,o.method_kind,o.outcome,o.retain_until,s.header_digest,s.header,s.payload_digest,s.phase,s.view_id,s.view_length FROM note_operation o LEFT JOIN note_stage s USING(operation_key) WHERE o.principal=? AND o.backend_id=? AND o.workspace_id=? AND o.note_id=? AND o.instance_id=? AND o.operation_id=?")
            .bind(principal).bind(&request.backend_id).bind(&request.workspace_id)
            .bind(&request.note_id).bind(&request.note_instance_id).bind(&request.operation_id)
            .fetch_optional(&mut *tx).await.map_err(db)?
            .ok_or_else(|| fail(NoteMutationError::Invalid))?;
        if row.get::<String, _>("method_kind") != "staged"
            || row.get::<Option<String>, _>("header_digest").as_deref()
                != Some(&request.header_digest)
            || row.get::<Option<String>, _>("payload_digest").as_deref()
                != Some(&request.payload_digest)
        {
            return Err(fail(NoteMutationError::Mismatch));
        }
        let now = intent_core::parse_iso(&intent_core::now_iso())
            .ok_or_else(|| fail(NoteMutationError::Invalid))?;
        let retain_until: i64 = row.get("retain_until");
        if retain_until <= now.unix_timestamp() {
            return Err(fail(NoteMutationError::Expired));
        }
        let state: Value = serde_json::from_str(row.get("outcome")).map_err(db)?;
        let phase: String = row.get("phase");
        if phase == "committed" {
            if state["kind"] != "noteCommitReceipt"
                || state["headerDigest"] != request.header_digest
                || state["payloadDigest"] != request.payload_digest
            {
                return Err(fail(NoteMutationError::Mismatch));
            }
            tx.commit().await.map_err(db)?;
            return Ok(StageCommitAdmission::Replay(state));
        }
        if phase != "sealed" {
            return Err(fail(NoteMutationError::Expired));
        }
        let expires_at = state["expiresAt"]
            .as_str()
            .ok_or_else(|| fail(NoteMutationError::Invalid))?
            .to_owned();
        let deadline =
            intent_core::parse_iso(&expires_at).ok_or_else(|| fail(NoteMutationError::Invalid))?;
        if deadline <= now {
            return Err(fail(NoteMutationError::Expired));
        }
        let header: NoteStageHeader = serde_json::from_str(row.get("header")).map_err(db)?;
        if header.action != NoteStageAction::Mutate {
            return Err(fail(NoteMutationError::Invalid));
        }
        let backend: String =
            sqlx::query_scalar("SELECT backend_id FROM note_page_backend WHERE singleton=1")
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        if backend != request.backend_id {
            return Err(fail(NoteMutationError::Conflict));
        }
        let head = sqlx::query("SELECT instance_id,current_rev,generation,indexed_rev FROM note_page_head WHERE workspace_id=? AND note_id=?")
            .bind(&request.workspace_id).bind(&request.note_id)
            .fetch_optional(&mut *tx).await.map_err(db)?
            .ok_or_else(|| fail(NoteMutationError::Conflict))?;
        let rev: i64 = head.get("current_rev");
        if head.get::<String, _>("instance_id") != request.note_instance_id
            || head.get::<i64, _>("indexed_rev") != rev
            || format!("r:{rev}:{}", head.get::<String, _>("generation")) != header.base_revision
        {
            return Err(fail(NoteMutationError::Conflict));
        }
        // The targeted note is required by canonical writes. Unrelated note
        // bodies are never fetched into this reservation.
        let note = sqlx::query("SELECT * FROM note WHERE workspace_id=? AND id=?")
            .bind(&request.workspace_id)
            .bind(&request.note_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        let note = crate::note_repo::map_note_row(&note)?;
        let operation_key: String = row.get("operation_key");
        let (generation,length): (i64,i64) = sqlx::query_as("SELECT generation,length FROM note_stage_view WHERE operation_key=? ORDER BY generation DESC LIMIT 1")
            .bind(&operation_key).fetch_one(&mut *tx).await.map_err(db)?;
        if length != row.get::<i64, _>("view_length") {
            return Err(fail(NoteMutationError::Invalid));
        }
        let identity = NoteApplySplices {
            backend_id: request.backend_id.clone(),
            workspace_id: request.workspace_id.clone(),
            note_id: request.note_id.clone(),
            note_instance_id: request.note_instance_id.clone(),
            base_revision: header.base_revision.clone(),
            operation_id: request.operation_id.clone(),
            expires_at,
            payload_digest: request.payload_digest.clone(),
            splices: Vec::new(),
        };
        Ok(StageCommitAdmission::Reserved(Box::new(
            StageCommitReservation {
                transaction: tx,
                identity,
                operation_key,
                note,
                header_digest: request.header_digest.clone(),
                view_id: row.get("view_id"),
                view_generation: u64::try_from(generation).map_err(db)?,
                view_length: u64::try_from(length).map_err(db)?,
                receipt_expires_at: intent_core::iso_from_unix_secs(retain_until),
            },
        )))
    }
}
