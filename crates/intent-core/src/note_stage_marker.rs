//! Pure staged marker shape checks. A successful result is NOT marker authority.
use crate::note_stage::{NoteStageRange, NoteStageRecord, NoteStageRole};
use serde_json::Value;

const SAFE: u64 = 9_007_199_254_740_991;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarkerKind {
    Start,
    End,
    Point,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarkerError {
    Invalid,
    Unsupported,
}

/// Borrowing these values establishes no ownership. The caller must resolve
/// descriptor and attributes from digest-verified resources in the same operation,
/// reject duplicate JSON members, and establish actual earlier-parent existence.
/// `frozen_literal` must be the exact scalar-aligned slice at `frozen_range` in
/// the bound immutable view, never a caller-supplied substitute of equal length.
///
/// Before admitting a marker, the caller MUST independently establish original
/// scoped live-comment ownership and contiguous original-root provenance for the
/// entire literal. Native schema/mapping, expiry and authorization remain separate.
pub struct ResolvedMarkerInput<'a> {
    pub record: &'a NoteStageRecord,
    pub descriptor: &'a Value,
    pub attributes: Option<&'a Value>,
    pub frozen_literal: &'a str,
    pub frozen_range: NoteStageRange,
    pub view_length: u64,
}

fn token(text: &str) -> bool {
    !text.is_empty() && text.len() <= 256 && !text.contains('\0')
}

/// Validate one individual literal, without pairing, recovery or identity minting.
/// Exact legacy IDs are preserved, including non-UUID IDs and colon spellings.
/// Work and temporary storage are bounded by the admitted 256-byte identifier.
/// # Errors
/// Returns Invalid for malformed geometry/attributes or literal mismatch, and
/// Unsupported for descriptor versions/node types/roles outside this adapter.
pub fn marker_literal(input: &ResolvedMarkerInput<'_>) -> Result<MarkerKind, MarkerError> {
    use MarkerError::{Invalid, Unsupported};
    let NoteStageRecord::Projection {
        ordinal,
        source_range,
        role,
        canonical_id,
        ..
    } = input.record
    else {
        return Err(Invalid);
    };
    if *role != NoteStageRole::MarkerOccurrence {
        return Err(Unsupported);
    }
    let id = canonical_id
        .as_deref()
        .filter(|id| token(id))
        .ok_or(Invalid)?;
    if *ordinal >= SAFE
        || input.view_length > SAFE
        || source_range.start >= source_range.end
        || source_range.end > input.view_length
        || source_range.start != input.frozen_range.start
        || source_range.end != input.frozen_range.end
    {
        return Err(Invalid);
    }
    let descriptor = input.descriptor.as_object().ok_or(Invalid)?;
    if descriptor.len() != 5
        || descriptor.get("version").and_then(Value::as_u64) != Some(1)
        || descriptor.get("nodeType").and_then(Value::as_str) != Some("commentAnchor")
    {
        return Err(Unsupported);
    }
    match descriptor.get("parentOrdinal") {
        Some(Value::Null) => (),
        Some(parent) if parent.as_u64().is_some_and(|parent| parent < *ordinal) => (),
        _ => return Err(Invalid),
    }
    if !descriptor
        .get("attributesRef")
        .and_then(Value::as_str)
        .is_some_and(token)
    {
        return Err(Invalid);
    }
    let native = descriptor
        .get("nativeRange")
        .and_then(Value::as_object)
        .ok_or(Invalid)?;
    let from = native.get("from").and_then(Value::as_u64).ok_or(Invalid)?;
    let to = native.get("to").and_then(Value::as_u64).ok_or(Invalid)?;
    if native.len() != 2 || from >= SAFE || to > SAFE || from.checked_add(1) != Some(to) {
        return Err(Invalid);
    }
    let attrs = input.attributes.and_then(Value::as_object).ok_or(Invalid)?;
    if attrs.len() != 3 || attrs.get("commentId").and_then(Value::as_str) != Some(id) {
        return Err(Invalid);
    }
    let (kind, spelling) = match attrs.get("type").and_then(Value::as_str) {
        Some("start") => (MarkerKind::Start, "start"),
        Some("end") => (MarkerKind::End, "end"),
        Some("point") => (MarkerKind::Point, "point"),
        _ => return Err(Invalid),
    };
    let atom_id = format!("{id}:{spelling}");
    if attrs.get("id").and_then(Value::as_str) != Some(atom_id.as_str()) {
        return Err(Invalid);
    }
    let literal = format!("<!--anchor:{id}:{spelling}-->");
    if input.frozen_literal != literal
        || u64::try_from(literal.encode_utf16().count()).map_err(|_| Invalid)?
            != source_range.end - source_range.start
    {
        return Err(Invalid);
    }
    Ok(kind)
}

#[cfg(test)]
#[path = "note_stage_marker_tests.rs"]
mod tests;
