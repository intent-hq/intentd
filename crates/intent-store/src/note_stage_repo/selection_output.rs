//! Resolve only the captured plain-paragraph selection subset from sealed storage.
//! The caller holds one scoped snapshot and owns current authorization, original
//! expiry, output admission and the final liveness check after these awaits.
use super::{db, fail, seal, view_read};
use intent_core::{
    note_mutation::NoteMutationError,
    note_stage::{
        NoteStageAction, NoteStageHeader, NoteStageOutput, NoteStageRange, NoteStageRecord,
        NoteStageRole, NoteStageSelection,
    },
    note_stage_selection_markdown::{
        selection_markdown, NoteSelectionMarkdownInput, ResolvedSelectionDescriptor,
        SelectionMarkdownError, PARAGRAPH_UNITS,
    },
    Error, Result,
};
use serde_json::{json, Value};
use sqlx::{Row, SqliteConnection};

fn invalid() -> Error {
    fail(NoteMutationError::Invalid)
}
fn unsupported() -> Error {
    Error::Unsupported("staged plain paragraph selection".into())
}
fn integer(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| invalid())
}

async fn records(
    conn: &mut SqliteConnection,
    operation: &str,
    stream: &str,
    count: usize,
) -> Result<Vec<NoteStageRecord>> {
    let declared: Option<i64> = sqlx::query_scalar(
        "SELECT records FROM note_stage_stream WHERE operation_key=? AND stream=?",
    )
    .bind(operation)
    .bind(stream)
    .fetch_optional(&mut *conn)
    .await
    .map_err(db)?;
    if declared != Some(i64::try_from(count).map_err(db)?) {
        return Err(unsupported());
    }
    let mut after = (-1, -1);
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        result.push(
            seal::next_record(conn, operation, stream, &mut after)
                .await?
                .ok_or_else(invalid)?,
        );
    }
    if seal::next_record(conn, operation, stream, &mut after)
        .await?
        .is_some()
    {
        return Err(invalid());
    }
    Ok(result)
}

// A resolved {} is produced only from the exact uploaded, digest-verified typed
// object and explicitly owned empty terminal directory. Missing attrs are never
// defaults. The seal's completed graph ledger is evidence, not arbitrary JSON.
async fn empty_attributes(conn: &mut SqliteConnection, operation: &str, id: &str) -> Result<Value> {
    let entry = seal::metadata_resource(conn, operation, id).await?;
    let object = entry.as_object().ok_or_else(unsupported)?;
    if object.len() != 4
        || entry["type"] != "object"
        || object.get("parentId") != Some(&Value::Null)
    {
        return Err(unsupported());
    }
    let entry_id = entry["id"].as_str().ok_or_else(unsupported)?;
    let directory = entry["childrenRef"].as_str().ok_or_else(unsupported)?;
    let valid:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM note_stage_validation WHERE operation_key=? AND kind='entry' AND id=? AND owner IS NULL AND state='done') AND EXISTS(SELECT 1 FROM note_stage_validation WHERE operation_key=? AND kind='entryId' AND id=? AND owner=?) AND EXISTS(SELECT 1 FROM note_stage_validation WHERE operation_key=? AND kind='directory' AND id=? AND owner=?)")
        .bind(operation).bind(id).bind(operation).bind(entry_id).bind(id).bind(operation).bind(directory).bind(entry_id)
        .fetch_one(&mut *conn).await.map_err(db)?;
    if !valid {
        return Err(invalid());
    }
    let value = seal::metadata_resource(conn, operation, directory).await?;
    if value != json!({"kind":"metadataChildren","items":[],"nextRef":null}) {
        return Err(unsupported());
    }
    Ok(json!({}))
}

async fn descriptor(
    conn: &mut SqliteConnection,
    operation: &str,
    generation: u64,
    record: &NoteStageRecord,
) -> Result<(Value, Value)> {
    let NoteStageRecord::Projection {
        ordinal,
        detail,
        source_range,
        role,
        canonical_id,
    } = record
    else {
        return Err(invalid());
    };
    seal::verify_reference(conn, operation, detail).await?;
    let ledger:Option<String>=sqlx::query_scalar("SELECT value FROM note_stage_validation WHERE operation_key=? AND kind='live' AND id=? AND owner=? AND state='done'")
        .bind(operation).bind(ordinal.to_string()).bind(&detail.text_id).fetch_optional(&mut *conn).await.map_err(db)?;
    let ledger = ledger.ok_or_else(invalid)?;
    if ledger.len() > 32768 {
        return Err(invalid());
    }
    let ledger: Value = serde_json::from_str(&ledger).map_err(db)?;
    if ledger["generation"].as_u64() != Some(generation) {
        return Err(invalid());
    }
    let value = seal::metadata_resource(conn, operation, &detail.text_id).await?;
    let attrs = value["attributesRef"].as_str().ok_or_else(unsupported)?;
    let role = match role {
        NoteStageRole::SelectionOwner => "selection-owner",
        NoteStageRole::ParagraphSeam => "paragraph-seam",
        NoteStageRole::InlineSpan => "inline-span",
        NoteStageRole::MarkerOccurrence => "marker-occurrence",
    };
    let mut expected = json!({"generation":generation,"sourceRange":{"start":source_range.start,"end":source_range.end},"nativeRange":value["nativeRange"],"parentOrdinal":value["parentOrdinal"],"nodeType":value["nodeType"],"role":role,"attributesRef":attrs});
    if let Some(id) = canonical_id {
        expected["canonicalId"] = json!(id);
    }
    if ledger != expected {
        return Err(invalid());
    }
    let attributes = empty_attributes(conn, operation, attrs).await?;
    Ok((value, attributes))
}

/// Bounded resources plus at most 4096 UTF16 units of frozen paragraph source.
/// This does not mint authority: the enclosing sealed-read snapshot must remain
/// immutable through publication. No output exists for internal no-copy results.
pub(super) async fn prepare(
    conn: &mut SqliteConnection,
    operation: &str,
    header: &NoteStageHeader,
    generation: u64,
    view_length: u64,
) -> Result<String> {
    header.validate().map_err(fail)?;
    if header.action != NoteStageAction::Read
        || header.output != NoteStageOutput::SelectionMarkdown
        || header.selection != NoteStageSelection::Ranges
    {
        return Err(unsupported());
    }
    let row=sqlx::query("SELECT s.header,s.phase,s.view_length,v.generation,v.length FROM note_stage s JOIN note_stage_view v USING(operation_key) WHERE s.operation_key=? ORDER BY v.generation DESC LIMIT 1")
        .bind(operation).fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(invalid)?;
    let retained: NoteStageHeader = serde_json::from_str(row.get("header")).map_err(db)?;
    if serde_json::to_value(retained).map_err(db)? != serde_json::to_value(header).map_err(db)?
        || row.get::<&str, _>("phase") != "sealed"
        || row.get::<Option<i64>, _>("view_length") != Some(integer(view_length)?)
        || row.get::<i64, _>("generation") != integer(generation)?
        || row.get::<i64, _>("length") != integer(view_length)?
    {
        return Err(invalid());
    }
    let selection = records(conn, operation, "selection", 1).await?;
    let live = records(conn, operation, "live", 2).await?;
    let (paragraph, paragraph_attrs) = descriptor(conn, operation, generation, &live[0]).await?;
    let (inline, inline_attrs) = descriptor(conn, operation, generation, &live[1]).await?;
    let NoteStageRecord::Projection { source_range, .. } = &live[0] else {
        return Err(invalid());
    };
    let range: NoteStageRange = *source_range;
    let units = range.end.checked_sub(range.start).ok_or_else(invalid)?;
    if range.end > view_length {
        return Err(invalid());
    }
    if units > u64::try_from(PARAGRAPH_UNITS).map_err(db)? {
        return Err(fail(NoteMutationError::Budget));
    }
    let mut source = String::new();
    let mut offset = range.start;
    while offset < range.end {
        // The primitive can hydrate one bounded piece beyond the requested end;
        // only exact scalar-aligned paragraph bytes enter the serializer.
        let (_, piece) =
            view_read::read_piece(conn, operation, generation, view_length, offset, 4096).await?;
        let before = offset;
        for scalar in piece.chars() {
            let next = offset
                .checked_add(u64::try_from(scalar.len_utf16()).map_err(db)?)
                .ok_or_else(invalid)?;
            if next > range.end {
                return Err(invalid());
            }
            source.push(scalar);
            offset = next;
            if offset == range.end {
                break;
            }
        }
        if offset == before {
            return Err(invalid());
        }
    }
    let resolved = [
        ResolvedSelectionDescriptor {
            record: &live[0],
            descriptor: &paragraph,
            attributes: &paragraph_attrs,
        },
        ResolvedSelectionDescriptor {
            record: &live[1],
            descriptor: &inline,
            attributes: &inline_attrs,
        },
    ];
    let input = NoteSelectionMarkdownInput {
        header,
        selection: &selection,
        live: &resolved,
        frozen_paragraph: &source,
        frozen_range: range,
        view_length,
    };
    let result = selection_markdown(&input).map_err(|error| match error {
        SelectionMarkdownError::Invalid => invalid(),
        SelectionMarkdownError::Unsupported => unsupported(),
        SelectionMarkdownError::Budget => fail(NoteMutationError::Budget),
    })?;
    result.text.map(str::to_owned).ok_or_else(unsupported)
}

#[cfg(test)]
#[path = "selection_output_tests.rs"]
mod tests;
