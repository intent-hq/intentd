//! Selection output for one inherited point atom between two plain text leaves.
//! The enclosing reader owns scope, header/view identity, authorization, original
//! expiry and final cancellation checks. This resolver uses only frozen evidence;
//! it never refreshes comment ownership or grants marker restoration authority.
use super::{db, fail, markers, seal, selection_output, view_read};
use intent_core::{
    note_mutation::NoteMutationError,
    note_stage::{NoteStageHeader, NoteStageRecord, NoteStageRole},
    note_stage_marker_selection::marker_selection_markdown,
    note_stage_selection_markdown::{
        NoteSelectionMarkdownInput, ResolvedSelectionDescriptor, SelectionMarkdownError,
        PARAGRAPH_UNITS,
    },
    Error, Result,
};
use serde_json::{json, Value};
use sqlx::{Row, SqliteConnection};

fn invalid() -> Error {
    fail(NoteMutationError::Invalid)
}
fn unsupported() -> Error {
    Error::Unsupported("staged inherited point selection".into())
}

async fn marker_descriptor(
    conn: &mut SqliteConnection,
    operation: &str,
    generation: u64,
    record: &NoteStageRecord,
) -> Result<(Value, Value)> {
    let NoteStageRecord::Projection {
        ordinal,
        source_range,
        role: NoteStageRole::MarkerOccurrence,
        canonical_id: Some(id),
        detail,
    } = record
    else {
        return Err(unsupported());
    };
    if *ordinal != 2 {
        return Err(unsupported());
    }
    if id.len() > 256
        || source_range.end.checked_sub(source_range.start)
            != Some(
                u64::try_from(format!("<!--anchor:{id}:point-->").encode_utf16().count())
                    .map_err(db)?,
            )
    {
        return Err(unsupported());
    }
    seal::verify_reference(conn, operation, detail).await?;
    let row = sqlx::query("SELECT v.value,s.marker_admission,s.root_key,s.view_id FROM note_stage_validation v JOIN note_stage s USING(operation_key) WHERE v.operation_key=? AND v.kind='live' AND v.id=? AND v.owner=? AND v.state='done' AND s.phase='sealed'")
        .bind(operation).bind(ordinal.to_string()).bind(&detail.text_id)
        .fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(invalid)?;
    let raw: &str = row.get("value");
    if raw.len() > 32768 {
        return Err(invalid());
    }
    let ledger: Value = serde_json::from_str(raw).map_err(db)?;
    let descriptor = seal::metadata_resource(conn, operation, &detail.text_id).await?;
    let attrs_ref = descriptor["attributesRef"]
        .as_str()
        .ok_or_else(unsupported)?;
    let attributes = markers::attributes(conn, operation, attrs_ref).await?;
    let admission: Value = serde_json::from_str(
        row.get::<Option<&str>, _>("marker_admission")
            .ok_or_else(unsupported)?,
    )
    .map_err(db)?;
    let witness = ledger.get("markerWitness").ok_or_else(unsupported)?;
    let root: &str = row.get("root_key");
    let view: &str = row.get("view_id");
    // Recheck the exact original-root interval in the immutable piece map. No
    // source is hydrated and no current comment/head lookup is performed.
    let original =
        markers::original_range(conn, operation, root, generation, *source_range).await?;
    let thread = witness["threadId"]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(invalid)?;
    let expected_witness = json!({"version":1,"admission":admission,"rootKey":root,
        "rootRange":{"start":original.start,"end":original.end},"canonicalId":id,
        "threadId":thread,"type":"point","viewId":view});
    if witness != &expected_witness {
        return Err(invalid());
    }
    let expected = json!({"generation":generation,
        "sourceRange":{"start":source_range.start,"end":source_range.end},
        "nativeRange":descriptor["nativeRange"],"parentOrdinal":descriptor["parentOrdinal"],
        "nodeType":"commentAnchor","role":"marker-occurrence","attributesRef":attrs_ref,
        "canonicalId":id,"markerWitness":expected_witness});
    if ledger != expected {
        return Err(invalid());
    }
    Ok((descriptor, attributes))
}

/// Called after the common reader verifies the captured header and sealed view.
/// Fixed four-record closure and a 4096-unit paragraph bound admit all allocation
/// before the disjoint selected text slices are joined into owned output.
pub(super) async fn prepare(
    conn: &mut SqliteConnection,
    operation: &str,
    header: &NoteStageHeader,
    generation: u64,
    view_length: u64,
) -> Result<String> {
    // This adapter is limited to a clean captured view. Dirty marker restoration
    // and edit provenance require a separate authority path.
    let dirty: i64 = sqlx::query_scalar(
        "SELECT records FROM note_stage_stream WHERE operation_key=? AND stream='dirty'",
    )
    .bind(operation)
    .fetch_one(&mut *conn)
    .await
    .map_err(db)?;
    if dirty != 0 {
        return Err(unsupported());
    }
    let selection = selection_output::records(conn, operation, "selection", 1).await?;
    let live = selection_output::records(conn, operation, "live", 4).await?;
    let NoteStageRecord::Projection { source_range, .. } = &live[0] else {
        return Err(invalid());
    };
    let range = *source_range;
    let units = range.end.checked_sub(range.start).ok_or_else(invalid)?;
    if range.end > view_length {
        return Err(invalid());
    }
    if units > u64::try_from(PARAGRAPH_UNITS).map_err(db)? {
        return Err(fail(NoteMutationError::Budget));
    }
    let mut resources = Vec::with_capacity(4);
    for (index, record) in live.iter().enumerate() {
        let NoteStageRecord::Projection { source_range, .. } = record else {
            return Err(invalid());
        };
        if source_range.start < range.start
            || source_range.end > range.end
            || source_range.start > source_range.end
        {
            return Err(invalid());
        }
        resources.push(if index == 2 {
            marker_descriptor(conn, operation, generation, record).await?
        } else {
            selection_output::descriptor(conn, operation, generation, record).await?
        });
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
    let resolved: Vec<_> = live
        .iter()
        .zip(&resources)
        .map(
            |(record, (descriptor, attributes))| ResolvedSelectionDescriptor {
                record,
                descriptor,
                attributes,
            },
        )
        .collect();
    marker_selection_markdown(&NoteSelectionMarkdownInput {
        header,
        selection: &selection,
        live: &resolved,
        frozen_paragraph: &source,
        frozen_range: range,
        view_length,
    })
    .map_err(|error| match error {
        SelectionMarkdownError::Invalid => invalid(),
        SelectionMarkdownError::Unsupported => unsupported(),
        SelectionMarkdownError::Budget => fail(NoteMutationError::Budget),
    })?
    .text
    .ok_or_else(unsupported)
}
