//! One transaction owns a partial write, its versions and durable retry outcome.
//! Dropping an unfinished mutation rolls it back; there is no commit-without-
//! receipt API. This is an internal seam, not the public canonical write service.
use intent_core::{
    note_mutation::{NoteApplySplices, NoteMutationError, NoteSourceHistory, NoteSplice},
    note_page::NoteScope,
    Comment, Error, Note, NoteVersionAuthor, Result,
};
use serde_json::{json, Value};
use sqlx::{Row, Sqlite, Transaction};

use crate::Store;

fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("note operation storage: {error}"))
}

fn fail(error: NoteMutationError) -> Error {
    Error::NoteMutation(error)
}

/// An exact replay has no write object and must not publish events again.
pub enum NoteMutationAdmission {
    Replay(Value),
    Write(Box<NoteMutationWrite>),
}

/// A write reservation, never held across RPCs. All business planning must use
/// this transaction's source/comments, not reread through another connection.
pub struct NoteMutationWrite {
    transaction: Transaction<'static, Sqlite>,
    request: NoteApplySplices,
    operation_key: String,
    note: Note,
    history: NoteSourceHistory,
    persisted_source: String,
    receipt_expires_at: String,
    effects: Vec<Value>,
    converted_count: u64,
    persisted_phases: u32,
    conversion: Option<ConversionSavepoint>,
}

struct ConversionSavepoint {
    note: Note,
    history: NoteSourceHistory,
    persisted_source: String,
    effects_len: usize,
    converted_count: u64,
    persisted_phases: u32,
}

impl Store {
    /// Acquire the writer, resolve exact replay before stale/expiry checks, and
    /// validate every caller range before allowing any canonical write phase.
    /// The service must authorize current workspace/note visibility first,
    /// including retained receipt access for a deleted incarnation.
    ///
    /// # Errors
    /// Returns bounded mutation failures or a storage failure; never a full Note
    /// in a stale-write error. An unfinished returned writer rolls back on drop.
    pub async fn begin_note_mutation(
        &self,
        principal: &str,
        request: NoteApplySplices,
        now: &str,
    ) -> Result<NoteMutationAdmission> {
        request.validate().map_err(fail)?;
        if principal.is_empty() || principal.len() > 256 || principal.contains('\0') {
            return Err(fail(NoteMutationError::Invalid));
        }
        let mut transaction = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db)?;
        let retained = lookup(
            &mut transaction,
            principal,
            &request.scope(),
            &request.operation_id,
        )
        .await?;
        if let Some((digest, outcome)) = retained {
            if digest != request.payload_digest {
                return Err(fail(NoteMutationError::Mismatch));
            }
            transaction.commit().await.map_err(db)?;
            return Ok(NoteMutationAdmission::Replay(outcome));
        }
        request
            .validate_new_admission(
                intent_core::parse_iso(now).ok_or_else(|| fail(NoteMutationError::Invalid))?,
            )
            .map_err(fail)?;
        let backend: String =
            sqlx::query_scalar("SELECT backend_id FROM note_page_backend WHERE singleton=1")
                .fetch_one(&mut *transaction)
                .await
                .map_err(db)?;
        if backend != request.backend_id {
            return Err(fail(NoteMutationError::Conflict));
        }
        let head = sqlx::query("SELECT instance_id,current_rev,generation,indexed_rev FROM note_page_head WHERE workspace_id=? AND note_id=?")
            .bind(&request.workspace_id).bind(&request.note_id)
            .fetch_optional(&mut *transaction).await.map_err(db)?
            .ok_or_else(|| Error::NotFound("Note not found".into()))?;
        let rev: i64 = head.try_get("current_rev").map_err(db)?;
        let generation: String = head.try_get("generation").map_err(db)?;
        if head.try_get::<String, _>("instance_id").map_err(db)? != request.note_instance_id
            || format!("r:{rev}:{generation}") != request.base_revision
            || head.try_get::<i64, _>("indexed_rev").map_err(db)? != rev
        {
            return Err(fail(NoteMutationError::Conflict));
        }
        let row = sqlx::query("SELECT * FROM note WHERE workspace_id=? AND id=?")
            .bind(&request.workspace_id)
            .bind(&request.note_id)
            .fetch_one(&mut *transaction)
            .await
            .map_err(db)?;
        let note = crate::note_repo::map_note_row(&row)?;
        // validate() admitted the inline budget; history tracks exact provenance.
        let mut history = NoteSourceHistory::new(note.content.clone());
        history.apply_phase(&request.splices).map_err(fail)?;
        let operation_key = uuid::Uuid::new_v4().to_string();
        let deadline = request.deadline().map_err(fail)?;
        // Round retention upward, never below seven days past a millisecond deadline.
        let retain_until = deadline.unix_timestamp() + 7 * 86_400 + 1;
        let receipt_expires_at = intent_core::iso_from_unix_secs(retain_until);
        let pending = json!({"kind":"noteOperationStatus","outcome":"pending", "scope":request.scope(),
            "operationId":request.operation_id,"payloadDigest":request.payload_digest});
        sqlx::query("INSERT INTO note_operation(operation_key,principal,backend_id,workspace_id,note_id,instance_id,operation_id,payload_digest,admission_expires,retain_until,outcome) VALUES(?,?,?,?,?,?,?,?,?,?,?)")
            .bind(&operation_key).bind(principal).bind(&request.backend_id).bind(&request.workspace_id)
            .bind(&request.note_id).bind(&request.note_instance_id).bind(&request.operation_id)
            .bind(&request.payload_digest).bind(deadline.unix_timestamp()).bind(retain_until)
            .bind(pending.to_string()).execute(&mut *transaction).await.map_err(db)?;
        copy_source(&mut transaction, &operation_key, "base", &request).await?;
        let persisted_source = note.content.clone();
        Ok(NoteMutationAdmission::Write(Box::new(NoteMutationWrite {
            transaction,
            request,
            operation_key,
            note,
            history,
            persisted_source,
            receipt_expires_at,
            effects: Vec::new(),
            converted_count: 0,
            persisted_phases: 0,
            conversion: None,
        })))
    }

    /// Read the exact retained outcome without loading source or a current note.
    /// This lookup deliberately does not authorize its caller; services must
    /// recheck workspace and original-incarnation visibility on every request.
    ///
    /// # Errors
    /// Rejects identity mismatch and storage failures.
    pub async fn note_mutation_status(
        &self,
        principal: &str,
        scope: &NoteScope,
        operation_id: &str,
        payload_digest: &str,
    ) -> Result<Value> {
        let mut tx = self.read_pool().begin().await.map_err(db)?;
        let retained = lookup(&mut tx, principal, scope, operation_id).await?;
        if let Some((digest, outcome)) = retained {
            if digest != payload_digest {
                return Err(fail(NoteMutationError::Mismatch));
            }
            Ok(outcome)
        } else {
            Ok(
                json!({"kind":"noteOperationStatus","outcome":"unknown","scope":scope,
                "operationId":operation_id,"payloadDigest":payload_digest}),
            )
        }
    }
}

async fn lookup(
    tx: &mut Transaction<'_, Sqlite>,
    principal: &str,
    scope: &NoteScope,
    operation_id: &str,
) -> Result<Option<(String, Value)>> {
    let row = sqlx::query("SELECT payload_digest,outcome FROM note_operation WHERE principal=? AND backend_id=? AND workspace_id=? AND note_id=? AND instance_id=? AND operation_id=?")
        .bind(principal).bind(&scope.backend_id).bind(&scope.workspace_id).bind(&scope.note_id)
        .bind(&scope.note_instance_id).bind(operation_id).fetch_optional(&mut **tx).await.map_err(db)?;
    row.map(|row| {
        Ok((
            row.try_get("payload_digest").map_err(db)?,
            serde_json::from_str(row.try_get("outcome").map_err(db)?).map_err(db)?,
        ))
    })
    .transpose()
}

async fn copy_source(
    tx: &mut Transaction<'_, Sqlite>,
    key: &str,
    phase: &str,
    request: &NoteApplySplices,
) -> Result<()> {
    sqlx::query("INSERT INTO note_operation_source(operation_key,phase,start,end,text) SELECT ?,?,start,end,text FROM note_page_piece WHERE workspace_id=? AND note_id=?")
        .bind(key).bind(phase).bind(&request.workspace_id).bind(&request.note_id)
        .execute(&mut **tx).await.map_err(db)?;
    Ok(())
}

impl NoteMutationWrite {
    #[must_use]
    pub fn note(&self) -> &Note {
        &self.note
    }

    #[must_use]
    pub fn source(&self) -> &str {
        self.history.source()
    }

    /// Read canonical anchor owners while the source writer lock is held.
    ///
    /// # Errors
    /// Returns a storage/legacy decoding error before publication.
    pub async fn comments(&mut self) -> Result<Vec<Comment>> {
        let rows = sqlx::query(
            "SELECT * FROM comment WHERE workspace_id=? AND note_id=? ORDER BY created_at,id",
        )
        .bind(&self.request.workspace_id)
        .bind(&self.request.note_id)
        .fetch_all(&mut *self.transaction)
        .await
        .map_err(db)?;
        rows.iter()
            .map(crate::comment_repo::map_comment_row)
            .collect()
    }

    /// Read conversion candidates in this write snapshot. This is deliberate
    /// document/workspace-sized write planning, never a paging read primitive.
    ///
    /// # Errors
    /// Returns a storage/legacy decoding error.
    pub async fn workspace_notes(&mut self) -> Result<Vec<Note>> {
        let rows = sqlx::query("SELECT * FROM note WHERE workspace_id=? ORDER BY created_at,id")
            .bind(&self.request.workspace_id)
            .fetch_all(&mut *self.transaction)
            .await
            .map_err(db)?;
        rows.iter().map(crate::note_repo::map_note_row).collect()
    }

    /// Flip only orphan state/time, retaining creation authorship and all legacy
    /// fields. Trigger-maintained annotation epochs share this transaction.
    ///
    /// # Errors
    /// Rejects a comment outside this note; any failure leaves work uncommitted.
    pub async fn mark_comments_orphaned(&mut self, ids: &[String], date: &str) -> Result<()> {
        for id in ids {
            let changed = sqlx::query("UPDATE comment SET extra_json=json_set(COALESCE(extra_json,'{}'),'$.isOrphaned',json('true')),updated_at=? WHERE id=? AND workspace_id=? AND note_id=? AND parent_id IS NULL")
                .bind(date).bind(id).bind(&self.request.workspace_id).bind(&self.request.note_id)
                .execute(&mut *self.transaction).await.map_err(db)?;
            if changed.rows_affected() != 1 {
                return Err(fail(NoteMutationError::Conflict));
            }
        }
        Ok(())
    }

    /// Insert a conversion child and its initial version inside the savepoint.
    /// Relations already validated by the service belong in its task metadata.
    ///
    /// # Errors
    /// Rejects a missing conversion phase, substituted parent or SQL failure.
    pub async fn insert_conversion_child(
        &mut self,
        child: &Note,
        author: &NoteVersionAuthor,
    ) -> Result<()> {
        if self.conversion.is_none()
            || child.workspace_id != self.note.workspace_id
            || child.parent_id.as_ref() != Some(&self.note.id)
            || child.id == self.note.id
        {
            return Err(fail(NoteMutationError::Invalid));
        }
        crate::note_repo::exec_insert_note(&mut self.transaction, child).await?;
        crate::note_version_repo::insert_note_version(
            &mut self.transaction,
            child,
            author,
            &child.updated_at,
            child.rev,
        )
        .await?;
        self.converted_count += 1;
        self.effects
            .push(json!({"kind":"createdTask","taskNoteId":child.id}));
        Ok(())
    }

    /// Apply declared canonical edits before persisting this phase. The service
    /// supplies bounded sourceEffect descriptors and uses actual parser ranges.
    ///
    /// # Errors
    /// Rejects invalid edits or an oversized immutable effect record.
    pub fn apply_canonical_phase(
        &mut self,
        edits: &[NoteSplice],
        effects: Vec<Value>,
    ) -> Result<()> {
        if effects
            .iter()
            .any(|effect| effect.to_string().len() > 32768)
        {
            return Err(fail(NoteMutationError::Budget));
        }
        self.history.apply_phase(edits).map_err(fail)?;
        self.effects.extend(effects);
        Ok(())
    }

    /// Persist one existing-semantics content snapshot inside the outer mutation.
    /// A subsequent conversion can add a separate version in this transaction.
    ///
    /// # Errors
    /// Returns storage failures; the entire mutation remains uncommitted.
    pub async fn persist_source(&mut self, author: &NoteVersionAuthor, date: &str) -> Result<()> {
        self.history.source().clone_into(&mut self.note.content);
        self.note.updated_at = date.into();
        let rev = crate::note_repo::exec_update_note(
            &mut self.transaction,
            &self.note,
            Some(self.note.rev),
            crate::note_repo::NoteUpdateScope::FullRow,
        )
        .await?
        .ok_or_else(|| fail(NoteMutationError::Conflict))?;
        crate::note_version_repo::insert_note_version(
            &mut self.transaction,
            &self.note,
            author,
            date,
            rev,
        )
        .await?;
        self.note.rev = rev;
        self.persisted_source.clone_from(&self.note.content);
        self.persisted_phases += 1;
        Ok(())
    }

    /// Start conversion only after the initial canonical write/history succeeds.
    ///
    /// # Errors
    /// Rejects nested conversion or an unpersisted initial phase.
    pub async fn begin_conversion(&mut self) -> Result<()> {
        if self.conversion.is_some()
            || self.persisted_source != self.source()
            || self.persisted_phases == 0
        {
            return Err(fail(NoteMutationError::Invalid));
        }
        sqlx::query("SAVEPOINT note_conversion")
            .execute(&mut *self.transaction)
            .await
            .map_err(db)?;
        self.conversion = Some(ConversionSavepoint {
            note: self.note.clone(),
            history: self.history.clone(),
            persisted_source: self.persisted_source.clone(),
            effects_len: self.effects.len(),
            converted_count: self.converted_count,
            persisted_phases: self.persisted_phases,
        });
        Ok(())
    }

    /// Discard only conversion writes/effects; the initial canonical phase stays.
    ///
    /// # Errors
    /// Rejects a missing savepoint or SQL rollback failure.
    pub async fn rollback_conversion(&mut self) -> Result<()> {
        if self.conversion.is_none() {
            return Err(fail(NoteMutationError::Invalid));
        }
        sqlx::query("ROLLBACK TO note_conversion")
            .execute(&mut *self.transaction)
            .await
            .map_err(db)?;
        sqlx::query("RELEASE note_conversion")
            .execute(&mut *self.transaction)
            .await
            .map_err(db)?;
        let saved = self
            .conversion
            .take()
            .ok_or_else(|| fail(NoteMutationError::Invalid))?;
        self.note = saved.note;
        self.history = saved.history;
        self.persisted_source = saved.persisted_source;
        self.effects.truncate(saved.effects_len);
        self.converted_count = saved.converted_count;
        self.persisted_phases = saved.persisted_phases;
        Ok(())
    }

    /// Retain a successfully persisted conversion phase in the outer write.
    ///
    /// # Errors
    /// Rejects an unfinished phase or SQL release failure.
    pub async fn finish_conversion(&mut self) -> Result<()> {
        if self.conversion.is_none() || self.persisted_source != self.source() {
            return Err(fail(NoteMutationError::Invalid));
        }
        sqlx::query("RELEASE note_conversion")
            .execute(&mut *self.transaction)
            .await
            .map_err(db)?;
        self.conversion = None;
        Ok(())
    }

    /// Write immutable mapping/inverse records and the receipt, then commit all
    /// source/index/history changes together. Only after this returns may the
    /// service publish invalidations; replay returns through a different arm.
    ///
    /// # Errors
    /// Unpersisted phases, oversized receipts or any storage failure roll back.
    pub async fn commit(mut self) -> Result<Value> {
        if self.conversion.is_some()
            || self.persisted_source != self.source()
            || self.persisted_phases == 0
        {
            return Err(fail(NoteMutationError::Invalid));
        }
        let head = sqlx::query("SELECT current_rev,generation,source_length FROM note_page_head WHERE workspace_id=? AND note_id=?")
            .bind(&self.request.workspace_id).bind(&self.request.note_id)
            .fetch_one(&mut *self.transaction).await.map_err(db)?;
        let revision = format!(
            "r:{}:{}",
            head.try_get::<i64, _>("current_rev").map_err(db)?,
            head.try_get::<String, _>("generation").map_err(db)?
        );
        if revision == self.request.base_revision {
            return Err(fail(NoteMutationError::Invalid));
        }
        let receipt = json!({"kind":"noteCommitReceipt","outcome":"committed","scope":self.request.scope(),
            "operationId":self.request.operation_id,"payloadDigest":self.request.payload_digest,
            "beforeRevision":self.request.base_revision,"afterRevision":revision,
            "sourceLength":head.try_get::<i64,_>("source_length").map_err(db)?,
            "mappingRef":format!("{}:mapping",self.operation_key),"effectsRef":format!("{}:effects",self.operation_key),
            "inverseRef":format!("{}:inverse",self.operation_key),"receiptExpiresAt":self.receipt_expires_at,"invalidation":"all"});
        // Reserve room for the maximum bounded RPC id and envelope framing.
        if receipt.to_string().len() > 3584 {
            return Err(fail(NoteMutationError::Budget));
        }
        let mapping = self.history.mapping();
        let mut base_position = 0;
        let mut final_position = 0;
        for (sequence, item) in mapping.iter().enumerate() {
            self.insert_item(
                "mapping",
                sequence,
                &serde_json::to_value(item).map_err(db)?,
            )
            .await?;
            final_position += item.start - base_position;
            let inverse = json!({"start":final_position,"end":final_position+item.inserted_length,
                "source":{"phase":"base","range":{"start":item.start,"end":item.end}}});
            self.insert_item("inverse", sequence, &inverse).await?;
            base_position = item.end;
            final_position += item.inserted_length;
        }
        for sequence in 0..self.effects.len() {
            self.insert_item("effects", sequence, &self.effects[sequence].clone())
                .await?;
        }
        copy_source(
            &mut self.transaction,
            &self.operation_key,
            "final",
            &self.request,
        )
        .await?;
        sqlx::query("UPDATE note_operation SET outcome=? WHERE operation_key=?")
            .bind(receipt.to_string())
            .bind(&self.operation_key)
            .execute(&mut *self.transaction)
            .await
            .map_err(db)?;
        self.transaction.commit().await.map_err(db)?;
        Ok(receipt)
    }

    async fn insert_item(&mut self, kind: &str, sequence: usize, value: &Value) -> Result<()> {
        sqlx::query(
            "INSERT INTO note_operation_item(operation_key,kind,sequence,value) VALUES(?,?,?,?)",
        )
        .bind(&self.operation_key)
        .bind(kind)
        .bind(i64::try_from(sequence).map_err(db)?)
        .bind(value.to_string())
        .execute(&mut *self.transaction)
        .await
        .map_err(db)?;
        Ok(())
    }
}
