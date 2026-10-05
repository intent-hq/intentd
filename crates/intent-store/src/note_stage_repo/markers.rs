//! Conservative inherited-marker admission inside the original seal writer.
//! Shape is not authority: require the unchanged ready ownership epoch, a live
//! scoped root, and contiguous original-root provenance for every literal byte.
//! The retained witness is historical evidence, never restoration authority.
use super::{db, fail, seal, view_read};
use intent_core::{
    note_mutation::NoteMutationError,
    note_stage::{NoteStageRange, NoteStageRecord, NoteStageRole},
    note_stage_marker::{marker_literal, MarkerError, ResolvedMarkerInput},
    Error, Result,
};
use serde_json::{json, Map, Value};
use sqlx::{Row, SqliteConnection};

#[cfg(test)]
#[derive(Default)]
pub(crate) struct MarkerSealPause {
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Notify,
}
#[cfg(test)]
tokio::task_local! {
    pub(crate) static MARKER_SEAL_PAUSE: std::sync::Arc<MarkerSealPause>;
}

fn invalid() -> Error {
    fail(NoteMutationError::Invalid)
}
fn unsupported() -> Error {
    Error::Unsupported("staged inherited marker provenance".into())
}
fn integer(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(db)
}

// The graph validator has already checked ordering, duplicate keys and ownership.
// This subset additionally requires one explicit three-string object directory;
// no defaults, generic graph coercion or unbounded resource reconstruction.
pub(super) async fn attributes(
    conn: &mut SqliteConnection,
    operation: &str,
    id: &str,
) -> Result<Value> {
    let root = seal::metadata_resource(conn, operation, id).await?;
    if root.as_object().is_none_or(|o| o.len() != 4)
        || root["type"] != "object"
        || root.get("parentId") != Some(&Value::Null)
    {
        return Err(unsupported());
    }
    let root_id = root["id"].as_str().ok_or_else(invalid)?;
    let directory = root["childrenRef"].as_str().ok_or_else(invalid)?;
    let done: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM note_stage_validation WHERE operation_key=? AND kind='entry' AND id=? AND owner IS NULL AND state='done') AND EXISTS(SELECT 1 FROM note_stage_validation WHERE operation_key=? AND kind='directory' AND id=? AND owner=?)")
        .bind(operation).bind(id).bind(operation).bind(directory).bind(root_id)
        .fetch_one(&mut *conn).await.map_err(db)?;
    if !done {
        return Err(invalid());
    }
    let directory = seal::metadata_resource(conn, operation, directory).await?;
    if directory.as_object().is_none_or(|o| o.len() != 3)
        || directory["kind"] != "metadataChildren"
        || directory.get("nextRef") != Some(&Value::Null)
    {
        return Err(unsupported());
    }
    let items = directory["items"].as_array().ok_or_else(invalid)?;
    if items.len() != 3 {
        return Err(unsupported());
    }
    let mut attrs = Map::new();
    for item in items {
        let id = item.as_str().ok_or_else(invalid)?;
        let child = seal::metadata_resource(conn, operation, id).await?;
        if child.as_object().is_none_or(|o| o.len() != 5)
            || child["type"] != "string"
            || child["parentId"] != root_id
        {
            return Err(unsupported());
        }
        let key = child["key"].as_str().ok_or_else(unsupported)?;
        if !matches!(key, "id" | "type" | "commentId") {
            return Err(unsupported());
        }
        let done: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM note_stage_validation WHERE operation_key=? AND kind='entry' AND id=? AND owner=? AND state='done')")
            .bind(operation).bind(id).bind(root_id).fetch_one(&mut *conn).await.map_err(db)?;
        if !done {
            return Err(invalid());
        }
        let text_id = child["valueRef"].as_str().ok_or_else(unsupported)?;
        // Bound before reading. The seal already verified the digest of every
        // staged text and the graph owns this reference in this operation.
        let length: Option<i64> = sqlx::query_scalar("SELECT length FROM note_stage_text WHERE operation_key=? AND text_id=? AND utf8_bytes<=262 AND sha256 IS NOT NULL")
            .bind(operation).bind(text_id).fetch_optional(&mut *conn).await.map_err(db)?;
        let length = u64::try_from(length.ok_or_else(unsupported)?).map_err(db)?;
        let mut text = String::new();
        let mut offset = 0;
        while offset < length {
            let (end, piece): (i64, String) = sqlx::query_as("SELECT end,text FROM note_stage_text_piece WHERE operation_key=? AND text_id=? AND start=? AND length(CAST(text AS BLOB))<=262")
                .bind(operation).bind(text_id).bind(integer(offset)?).fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(invalid)?;
            let end = u64::try_from(end).map_err(db)?;
            if end <= offset || end > length || text.len() + piece.len() > 262 {
                return Err(invalid());
            }
            text.push_str(&piece);
            offset = end;
        }
        if attrs.insert(key.to_owned(), Value::String(text)).is_some() {
            return Err(invalid());
        }
    }
    Ok(Value::Object(attrs))
}

pub(super) async fn original_range(
    conn: &mut SqliteConnection,
    operation: &str,
    root: &str,
    generation: u64,
    range: NoteStageRange,
) -> Result<NoteStageRange> {
    let mut position = range.start;
    let mut original = None;
    while position < range.end {
        let row = sqlx::query("SELECT start,end,origin_kind,origin_id,origin_start FROM note_stage_view_piece WHERE operation_key=? AND generation=? AND start<=? ORDER BY start DESC LIMIT 1")
            .bind(operation).bind(integer(generation)?).bind(integer(position)?)
            .fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(invalid)?;
        let start = u64::try_from(row.get::<i64, _>("start")).map_err(db)?;
        let end = u64::try_from(row.get::<i64, _>("end")).map_err(db)?;
        if start > position || end <= position {
            return Err(invalid());
        }
        if row.get::<&str, _>("origin_kind") != "root" || row.get::<&str, _>("origin_id") != root {
            return Err(unsupported());
        }
        let mapped = u64::try_from(row.get::<i64, _>("origin_start"))
            .map_err(db)?
            .checked_add(position - start)
            .ok_or_else(invalid)?;
        let original_start = *original.get_or_insert(mapped);
        if original_start.checked_add(position - range.start) != Some(mapped) {
            return Err(unsupported());
        }
        position = end.min(range.end);
    }
    let start = original.ok_or_else(invalid)?;
    Ok(NoteStageRange {
        start,
        end: start
            .checked_add(range.end - range.start)
            .ok_or_else(invalid)?,
    })
}

pub(super) async fn validate(
    conn: &mut SqliteConnection,
    operation: &str,
    root: &str,
    view: &seal::PreparedView,
) -> Result<()> {
    let mut after = (-1, -1);
    let mut admission: Option<Value> = None;
    let mut previous_end = 0;
    while let Some(record) = seal::next_record(conn, operation, "live", &mut after).await? {
        let NoteStageRecord::Projection {
            ordinal,
            source_range,
            role: NoteStageRole::MarkerOccurrence,
            canonical_id,
            detail,
        } = &record
        else {
            continue;
        };
        if admission.is_none() {
            let row = sqlx::query("SELECT s.marker_admission,json_object('headId',a.id,'sourceRev',a.source_rev,'commentRevision',a.comment_revision,'stateGeneration',st.state_generation,'sourceRevision',st.source_revision) AS current_admission FROM note_stage s JOIN note_operation o USING(operation_key) JOIN note_annotation_head a ON a.workspace_id=o.workspace_id AND a.note_id=o.note_id JOIN note_page_head p ON p.workspace_id=o.workspace_id AND p.note_id=o.note_id JOIN note_annotation_state st ON st.workspace_id=o.workspace_id AND st.note_id=o.note_id WHERE s.operation_key=? AND s.root_key=? AND p.instance_id=o.instance_id AND a.source_rev=p.current_rev AND a.anchors_rev=a.source_rev AND st.instance_id=p.instance_id AND st.deleted=0 AND st.source_revision=s.base_revision AND st.comment_revision=a.comment_revision")
                .bind(operation).bind(root).fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(unsupported)?;
            let captured = row
                .get::<Option<&str>, _>("marker_admission")
                .ok_or_else(unsupported)?;
            if captured != row.get::<&str, _>("current_admission") {
                return Err(unsupported());
            }
            admission = Some(serde_json::from_str::<Value>(captured).map_err(db)?);
        }
        let canonical_id = canonical_id.as_deref().ok_or_else(invalid)?;
        // Current equality with the captured epoch makes this scoped lookup
        // evidence about the original owner. A later sealed replay skips it.
        let thread: Option<String> = sqlx::query_scalar("SELECT c.thread_id FROM comment c JOIN note_operation o ON o.workspace_id=c.workspace_id AND o.note_id=c.note_id WHERE o.operation_key=? AND c.id=? AND c.parent_id IS NULL AND COALESCE(json_type(c.extra_json,'$.isOrphaned'),'null')!='true'")
            .bind(operation).bind(canonical_id).fetch_optional(&mut *conn).await.map_err(db)?;
        let thread = thread.ok_or_else(unsupported)?;
        let descriptor = seal::metadata_resource(conn, operation, &detail.text_id).await?;
        let attrs = attributes(
            conn,
            operation,
            descriptor["attributesRef"]
                .as_str()
                .ok_or_else(unsupported)?,
        )
        .await?;
        let spelling = attrs["type"].as_str().ok_or_else(invalid)?;
        if !matches!(spelling, "start" | "end" | "point") || canonical_id.len() > 256 {
            return Err(invalid());
        }
        let expected = format!("<!--anchor:{canonical_id}:{spelling}-->");
        if source_range.start < previous_end {
            return Err(unsupported());
        }
        let (end, literal) = view_read::read_piece(
            conn,
            operation,
            view.generation,
            view.length,
            source_range.start,
            expected.len(),
        )
        .await?;
        if end != source_range.end {
            return Err(invalid());
        }
        marker_literal(&ResolvedMarkerInput {
            record: &record,
            descriptor: &descriptor,
            attributes: Some(&attrs),
            frozen_literal: &literal,
            frozen_range: *source_range,
            view_length: view.length,
        })
        .map_err(|error| match error {
            MarkerError::Invalid => invalid(),
            MarkerError::Unsupported => unsupported(),
        })?;
        let original =
            original_range(conn, operation, root, view.generation, *source_range).await?;
        let witness = json!({"version":1,"admission":admission,"rootKey":root,"rootRange":{"start":original.start,"end":original.end},"canonicalId":canonical_id,"threadId":thread,"type":spelling,"viewId":view.view_id});
        let result = sqlx::query("UPDATE note_stage_validation SET value=json_set(value,'$.markerWitness',json(?)) WHERE operation_key=? AND kind='live' AND id=? AND owner=? AND state='done'")
            .bind(witness.to_string()).bind(operation).bind(ordinal.to_string()).bind(&detail.text_id).execute(&mut *conn).await.map_err(db)?;
        if result.rows_affected() != 1 {
            return Err(invalid());
        }
        previous_end = source_range.end;
    }
    #[cfg(test)]
    if admission.is_some() {
        if let Ok(pause) = MARKER_SEAL_PAUSE.try_with(std::sync::Arc::clone) {
            pause.entered.notify_one();
            pause.release.notified().await;
        }
    }
    Ok(())
}
