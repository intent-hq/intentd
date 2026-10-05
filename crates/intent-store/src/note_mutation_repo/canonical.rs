//! Durable canonical phase provenance owned by the same mutation transaction.
use super::{db, fail, NoteMutationWrite};
use crate::note_annotation_repo::{self, AnchorOccurrence};
use intent_core::{
    note_mutation::{NoteMutationError, NoteSplice},
    Result,
};
use serde_json::json;
use sha2::{Digest, Sha256};

fn byte_at(source: &str, offset: u64) -> Result<usize> {
    let mut units = 0;
    for (byte, ch) in source.char_indices() {
        if units == offset {
            return Ok(byte);
        }
        units += ch.len_utf16() as u64;
        if units > offset {
            return Err(fail(NoteMutationError::Invalid));
        }
    }
    if units == offset {
        Ok(source.len())
    } else {
        Err(fail(NoteMutationError::Invalid))
    }
}

impl NoteMutationWrite {
    // Hash retained text via indexed scalar-safe pieces; never hydrate an entire
    // inverse value merely to mint its immutable reference.
    pub(super) async fn retain_text_reference(
        &mut self,
        text_id: &str,
        phase: &str,
        start: u64,
        end: u64,
    ) -> Result<serde_json::Value> {
        let mut position = start;
        let mut hasher = Sha256::new();
        let mut bytes = 0u64;
        while position < end {
            let row: Option<(i64, i64, String)> = sqlx::query_as("SELECT start,end,text FROM note_operation_source WHERE operation_key=? AND phase=? AND start<=? AND end>? ORDER BY start DESC LIMIT 1")
                .bind(&self.operation_key).bind(phase).bind(i64::try_from(position).map_err(db)?).bind(i64::try_from(position).map_err(db)?)
                .fetch_optional(&mut *self.transaction).await.map_err(db)?;
            let (piece_start, piece_end, text) =
                row.ok_or_else(|| fail(NoteMutationError::Invalid))?;
            let piece_start = u64::try_from(piece_start).map_err(db)?;
            let next = end.min(u64::try_from(piece_end).map_err(db)?);
            let fragment =
                &text[byte_at(&text, position - piece_start)?..byte_at(&text, next - piece_start)?];
            hasher.update(fragment.as_bytes());
            bytes += fragment.len() as u64;
            position = next;
        }
        let digest = format!("{:x}", hasher.finalize());
        sqlx::query("INSERT INTO note_operation_text(operation_key,text_id,phase,start,end,length,utf8_bytes,sha256) VALUES(?,?,?,?,?,?,?,?)")
            .bind(&self.operation_key).bind(text_id).bind(phase).bind(i64::try_from(start).map_err(db)?).bind(i64::try_from(end).map_err(db)?)
            .bind(i64::try_from(end-start).map_err(db)?).bind(i64::try_from(bytes).map_err(db)?).bind(&digest).execute(&mut *self.transaction).await.map_err(db)?;
        Ok(json!({"textId":text_id,"length":end-start,"utf8Bytes":bytes,"sha256":digest}))
    }

    async fn register_reference(&mut self, reference: &str) -> Result<()> {
        sqlx::query(
            "INSERT OR IGNORE INTO note_operation_reference(operation_key,reference) VALUES(?,?)",
        )
        .bind(&self.operation_key)
        .bind(reference)
        .execute(&mut *self.transaction)
        .await
        .map_err(db)?;
        Ok(())
    }

    async fn insert_detail(
        &mut self,
        reference: &str,
        sequence: usize,
        value: &serde_json::Value,
    ) -> Result<()> {
        self.register_reference(reference).await?;
        for field in ["childrenRef", "valueRef", "keyRef", "nextRef"] {
            if let Some(reference) = value.get(field).and_then(serde_json::Value::as_str) {
                self.register_reference(reference).await?;
            }
        }
        let encoded = value.to_string();
        if encoded.len() > 32768 {
            return Err(fail(NoteMutationError::Budget));
        }
        sqlx::query("INSERT INTO note_operation_detail(operation_key,reference,sequence,value) VALUES(?,?,?,?)")
            .bind(&self.operation_key).bind(reference).bind(i64::try_from(sequence).map_err(db)?).bind(encoded)
            .execute(&mut *self.transaction).await.map_err(db)?;
        Ok(())
    }

    // Persist the existing metadata-tree envelope for actual source provenance.
    // Object children are sorted; oversized strings use scalar-safe fragments.
    pub(super) async fn retain_detail_tree(
        &mut self,
        reference: &str,
        value: serde_json::Value,
    ) -> Result<()> {
        use std::collections::VecDeque;
        let mut queue = VecDeque::from([(
            reference.to_owned(),
            0usize,
            serde_json::Value::Null,
            None::<String>,
            None::<usize>,
            value,
        )]);
        let mut node_index = 0usize;
        while let Some((page, sequence, parent, key, index, value)) = queue.pop_front() {
            let id = format!("{reference}:n{node_index}");
            node_index += 1;
            let mut entry = json!({"id":id,"parentId":parent});
            if let Some(key) = key.as_ref() {
                entry["key"] = json!(key);
            }
            if let Some(index) = index {
                entry["index"] = json!(index);
            }
            match value {
                serde_json::Value::Object(object) => {
                    let children = format!("{id}:children");
                    entry["type"] = json!("object");
                    entry["childrenRef"] = json!(children);
                    let mut fields: Vec<_> = object.into_iter().collect();
                    fields.sort_by(|a, b| a.0.cmp(&b.0));
                    for (ordinal, (key, value)) in fields.into_iter().enumerate() {
                        queue.push_back((
                            children.clone(),
                            ordinal,
                            json!(id),
                            Some(key),
                            None,
                            value,
                        ));
                    }
                }
                serde_json::Value::Array(array) => {
                    let children = format!("{id}:children");
                    entry["type"] = json!("array");
                    entry["childrenRef"] = json!(children);
                    for (ordinal, value) in array.into_iter().enumerate() {
                        queue.push_back((
                            children.clone(),
                            ordinal,
                            json!(id),
                            None,
                            Some(ordinal),
                            value,
                        ));
                    }
                }
                serde_json::Value::String(text) => {
                    entry["type"] = json!("string");
                    if text.len() <= 1024 {
                        entry["value"] = json!(text);
                    } else {
                        let reference = format!("{id}:value");
                        let phase = format!("d:{id}");
                        entry["valueRef"] = json!(reference);
                        self.retain_text(&phase, &text).await?;
                        sqlx::query("INSERT INTO note_operation_scalar(operation_key,reference,id,field,phase,length) VALUES(?,?,?,?,?,?)")
                            .bind(&self.operation_key).bind(&reference).bind(&id).bind(key.as_deref().unwrap_or("value")).bind(&phase)
                            .bind(i64::try_from(text.encode_utf16().count()).map_err(db)?).execute(&mut *self.transaction).await.map_err(db)?;
                    }
                }
                serde_json::Value::Number(number) => {
                    entry["type"] = json!("number");
                    entry["value"] = json!(number);
                }
                serde_json::Value::Bool(value) => {
                    entry["type"] = json!("boolean");
                    entry["value"] = json!(value);
                }
                serde_json::Value::Null => {
                    entry["type"] = json!("null");
                    entry["value"] = serde_json::Value::Null;
                }
            }
            self.insert_detail(&page, sequence, &entry).await?;
        }
        Ok(())
    }

    /// Retain exact scalar-safe source pieces for receipt-owned context reads.
    /// This is document-sized write work, never a page-read operation.
    ///
    /// # Errors
    /// SQL failure must propagate or be rolled back with the conversion savepoint.
    pub async fn retain_source_state(&mut self, phase: &str) -> Result<()> {
        let source = self.source().to_owned();
        self.retain_text(phase, &source).await
    }

    async fn retain_text(&mut self, phase: &str, source: &str) -> Result<()> {
        let mut byte = 0;
        let mut start = 0i64;
        while byte < source.len() {
            let mut end = (byte + 4096).min(source.len());
            while !source.is_char_boundary(end) {
                end -= 1;
            }
            let text = &source[byte..end];
            let next = start + i64::try_from(text.encode_utf16().count()).map_err(db)?;
            sqlx::query("INSERT INTO note_operation_source(operation_key,phase,start,end,text) VALUES(?,?,?,?,?)")
                .bind(&self.operation_key).bind(phase).bind(start).bind(next).bind(text)
                .execute(&mut *self.transaction).await.map_err(db)?;
            byte = end;
            start = next;
        }
        Ok(())
    }

    /// Record every actual canonical replacement with its phase coordinates and
    /// exact UTF-8 digests, then apply it to the source provenance composition.
    ///
    /// # Errors
    /// Invalid edits or storage failures leave the outer mutation uncommitted.
    pub async fn apply_recorded_phase(&mut self, reason: &str, edits: &[NoteSplice]) -> Result<()> {
        if !matches!(
            reason,
            "anchor-repair" | "phantom-scrub" | "task-conversion" | "task-marker-projection"
        ) {
            return Err(fail(NoteMutationError::Invalid));
        }
        if edits.is_empty() {
            return Ok(());
        }
        let input = self.source().to_owned();
        let sequence = self.effects.len();
        let input_state = format!("e:{sequence}:in");
        let output_state = format!("e:{sequence}:out");
        let mut effects = Vec::with_capacity(edits.len());
        for (index, edit) in edits.iter().enumerate() {
            let removed = &input[byte_at(&input, edit.start)?..byte_at(&input, edit.end)?];
            let detail = format!("{}:effect:{}", self.operation_key, sequence + index);
            self.retain_detail_tree(&detail, json!({"inputState":input_state,"outputState":output_state,
                "range":{"start":edit.start,"end":edit.end},"removed":removed,"inserted":edit.text})).await?;
            effects.push(json!({"kind":"sourceEffect","reason":reason,
                "inputState":input_state,"outputState":output_state,
                "range":{"start":edit.start,"end":edit.end},
                "insertedLength":edit.text.encode_utf16().count(),
                "beforeDigest":format!("{:x}",Sha256::digest(removed.as_bytes())),
                "afterDigest":format!("{:x}",Sha256::digest(edit.text.as_bytes())),
                "detailRef":format!("{}:effect:{}",self.operation_key,sequence+index)}));
        }
        self.apply_canonical_phase(edits, effects)?;
        self.retain_text(&input_state, &input).await?;
        self.retain_source_state(&output_state).await
    }

    /// Preserve business warning text without putting it in the acknowledgement.
    ///
    /// # Errors
    /// Returns storage failure; the caller must not commit a partially failed phase.
    pub async fn record_warning(&mut self, code: &str, warning: &str) -> Result<()> {
        let mut end = warning.len().min(256);
        while !warning.is_char_boundary(end) {
            end -= 1;
        }
        let sequence = self.effects.len();
        let mut effect = json!({"kind":"warning","code":code,"messagePreview":&warning[..end],"truncated":end<warning.len()});
        if end < warning.len() {
            effect["detailRef"] = json!(format!("{}:warning:{sequence}", self.operation_key));
            self.retain_detail_tree(
                &format!("{}:warning:{sequence}", self.operation_key),
                json!({"message":warning}),
            )
            .await?;
            self.retain_text(&format!("warning:{sequence}"), warning)
                .await?;
        }
        self.effects.push(effect);
        Ok(())
    }

    /// Publish canonical marker occurrences and their invalidation tuple in the
    /// same transaction as source, versions and the receipt.
    ///
    /// # Errors
    /// Rejects invalid occurrences or source/annotation storage failures.
    pub async fn publish_annotation_anchors(
        &mut self,
        occurrences: &[AnchorOccurrence],
    ) -> Result<()> {
        let (_, epochs) = note_annotation_repo::head(
            &mut self.transaction,
            &self.note.workspace_id,
            &self.note.id,
        )
        .await?;
        note_annotation_repo::publish_anchors_in_transaction(
            &mut self.transaction,
            &self.note.workspace_id,
            &self.note.id,
            &epochs,
            occurrences,
        )
        .await?;
        let (rev, generation): (i64, String) = sqlx::query_as(
            "SELECT current_rev,generation FROM note_page_head WHERE workspace_id=? AND note_id=?",
        )
        .bind(&self.request.workspace_id)
        .bind(&self.request.note_id)
        .fetch_one(&mut *self.transaction)
        .await
        .map_err(db)?;
        self.effects.push(json!({"kind":"annotationInvalidation","sourceRevision":format!("r:{rev}:{generation}"),
            "attributionGeneration":epochs.attribution_generation,"commentRevision":epochs.comment_revision}));
        Ok(())
    }
}
