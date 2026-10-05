//! Resolve the fixed whole-paragraph rendered identity subset from owned resources.
//! This is separate from primary source/selection output and grants no authorization.
use super::{db, fail, seal, selection_output, view_read};
use intent_core::{
    note_mutation::NoteMutationError,
    note_stage::{NoteStageHeader, NoteStageRange, NoteStageRecord, NoteStageTextReference},
    note_stage_rendered::{
        rendered_identity, NoteRenderedInput, RenderedError, RENDERED_BYTES, RENDERED_UNITS,
    },
    note_stage_selection_markdown::ResolvedSelectionDescriptor,
    Error, Result,
};
use serde_json::Value;
use sqlx::{Row, SqliteConnection};

fn invalid() -> Error {
    fail(NoteMutationError::Invalid)
}
fn integer(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(db)
}

pub(super) struct Capture {
    pub(super) text: String,
    pub(super) source_range: NoteStageRange,
    pub(super) selected_range: NoteStageRange,
    pub(super) parent: Value,
    pub(super) leaf: Value,
}

impl Capture {
    pub(super) fn piece(&self, at: u64, max_bytes: usize) -> Result<String> {
        if at < self.source_range.start || at > self.source_range.end {
            return Err(invalid());
        }
        let mut position = self.source_range.start;
        let mut output = String::new();
        for scalar in self.text.chars() {
            let next = position + u64::try_from(scalar.len_utf16()).map_err(db)?;
            if position < at && at < next {
                return Err(invalid());
            }
            if position >= at {
                if output.len() + scalar.len_utf8() > max_bytes {
                    break;
                }
                output.push(scalar);
            }
            position = next;
        }
        if output.is_empty() && at < self.source_range.end {
            return Err(fail(NoteMutationError::Budget));
        }
        Ok(output)
    }
}

async fn text_resource(
    conn: &mut SqliteConnection,
    operation: &str,
    reference: &NoteStageTextReference,
) -> Result<String> {
    if reference.length > u64::try_from(RENDERED_UNITS).map_err(db)?
        || reference.utf8_bytes > u64::try_from(RENDERED_BYTES).map_err(db)?
    {
        return Err(fail(NoteMutationError::Budget));
    }
    seal::verify_reference(conn, operation, reference).await?;
    let mut text = String::new();
    let mut offset = 0;
    while offset < reference.length {
        let row=sqlx::query("SELECT end,CASE WHEN length(CAST(text AS BLOB))<=4096 THEN text END AS text FROM note_stage_text_piece WHERE operation_key=? AND text_id=? AND start=?")
            .bind(operation).bind(&reference.text_id).bind(integer(offset)?).fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(invalid)?;
        let end = u64::try_from(row.get::<i64, _>("end")).map_err(db)?;
        let piece = row.get::<Option<&str>, _>("text").ok_or_else(invalid)?;
        if end <= offset
            || end > reference.length
            || u64::try_from(piece.encode_utf16().count()).map_err(db)? != end - offset
            || text.len().saturating_add(piece.len()) > RENDERED_BYTES
        {
            return Err(invalid());
        }
        text.push_str(piece);
        offset = end;
    }
    Ok(text)
}

/// Fixed resource subset limits are separate from per-page output/scan budgets.
/// `preparing` is used only inside the original seal writer transaction.
pub(super) async fn prepare(
    conn: &mut SqliteConnection,
    operation: &str,
    header: &NoteStageHeader,
    generation: u64,
    view_length: u64,
    preparing: bool,
) -> Result<Capture> {
    let row=sqlx::query("SELECT s.header,s.phase,s.view_length,v.generation,v.length FROM note_stage s JOIN note_stage_view v USING(operation_key) WHERE s.operation_key=? ORDER BY v.generation DESC LIMIT 1")
        .bind(operation).fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(invalid)?;
    let retained: NoteStageHeader = serde_json::from_str(row.get("header")).map_err(db)?;
    if serde_json::to_value(retained).map_err(db)? != serde_json::to_value(header).map_err(db)?
        || row.get::<&str, _>("phase") != if preparing { "staging" } else { "sealed" }
        || (!preparing && row.get::<Option<i64>, _>("view_length") != Some(integer(view_length)?))
        || row.get::<i64, _>("generation") != integer(generation)?
        || row.get::<i64, _>("length") != integer(view_length)?
    {
        return Err(invalid());
    }
    let selection = selection_output::records(conn, operation, "selection", 1).await?;
    let live = selection_output::records(conn, operation, "live", 2).await?;
    let (parent, parent_attrs) =
        selection_output::descriptor(conn, operation, generation, &live[0]).await?;
    let (leaf, leaf_attrs) =
        selection_output::descriptor(conn, operation, generation, &live[1]).await?;
    let reference: NoteStageTextReference =
        serde_json::from_value(leaf["renderedText"].clone()).map_err(|_| invalid())?;
    let text = text_resource(conn, operation, &reference).await?;
    let NoteStageRecord::Projection { source_range, .. } = &live[0] else {
        return Err(invalid());
    };
    let range = *source_range;
    if range.start > range.end || range.end > view_length {
        return Err(invalid());
    }
    if range.end - range.start > u64::try_from(RENDERED_UNITS).map_err(db)? {
        return Err(fail(NoteMutationError::Budget));
    }
    let mut source = String::new();
    let mut offset = range.start;
    while offset < range.end {
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
            descriptor: &parent,
            attributes: &parent_attrs,
        },
        ResolvedSelectionDescriptor {
            record: &live[1],
            descriptor: &leaf,
            attributes: &leaf_attrs,
        },
    ];
    let input = NoteRenderedInput {
        header,
        selection: &selection,
        live: &resolved,
        frozen_paragraph: &source,
        frozen_range: range,
        view_length,
        rendered_reference: &reference,
        rendered_text: &text,
    };
    let validated = rendered_identity(&input).map_err(|e| match e {
        RenderedError::Invalid => invalid(),
        RenderedError::Unsupported => Error::Unsupported("staged rendered identity capture".into()),
        RenderedError::Budget => fail(NoteMutationError::Budget),
    })?;
    let source_range = validated.source_range;
    let selected_range = validated.selected_range;
    Ok(Capture {
        text,
        source_range,
        selected_range,
        parent,
        leaf,
    })
}
