//! Pure copy semantics for the captured text/point-marker/text paragraph subset.
//! This helper neither authenticates resources nor establishes marker ownership.
use crate::note_stage::{
    NoteStageAction, NoteStageAffinity, NoteStageDirection, NoteStageOutput, NoteStageRange,
    NoteStageRecord, NoteStageRole, NoteStageSelection,
};
use crate::note_stage_marker::{marker_literal, MarkerError, MarkerKind, ResolvedMarkerInput};
use crate::note_stage_selection_markdown::{
    NoteSelectionMarkdownInput, ResolvedSelectionDescriptor, SelectionMarkdownError,
};
use serde_json::Value;
use SelectionMarkdownError::{Budget, Invalid, Unsupported};

type Result<T> = std::result::Result<T, SelectionMarkdownError>;
const SAFE: u64 = 9_007_199_254_740_991;

/// None is no-copy, never authorization to publish an empty clipboard write.
#[derive(Debug)]
pub struct NoteMarkerSelectionMarkdown {
    pub text: Option<String>,
    pub source_range: NoteStageRange,
    pub direction: NoteStageDirection,
    pub anchor_affinity: NoteStageAffinity,
    pub head_affinity: NoteStageAffinity,
}
struct Geometry {
    source: NoteStageRange,
    from: u64,
    to: u64,
}

fn geometry(
    input: &ResolvedSelectionDescriptor<'_>,
    ordinal: u64,
    role: NoteStageRole,
    node: &str,
) -> Result<Geometry> {
    let NoteStageRecord::Projection {
        ordinal: actual,
        source_range,
        role: actual_role,
        canonical_id,
        ..
    } = input.record
    else {
        return Err(Invalid);
    };
    if *actual != ordinal || source_range.start > source_range.end || source_range.end > SAFE {
        return Err(Invalid);
    }
    if *actual_role != role || (ordinal != 2 && canonical_id.is_some()) {
        return Err(Unsupported);
    }
    let object = input.descriptor.as_object().ok_or(Invalid)?;
    if object.len() != 5
        || object.get("version").and_then(Value::as_u64) != Some(1)
        || object.get("nodeType").and_then(Value::as_str) != Some(node)
    {
        return Err(Unsupported);
    }
    let parent = object.get("parentOrdinal").ok_or(Invalid)?;
    if (ordinal == 0 && !parent.is_null()) || (ordinal != 0 && parent.as_u64() != Some(0)) {
        return Err(Invalid);
    }
    if !object
        .get("attributesRef")
        .and_then(Value::as_str)
        .is_some_and(|s| !s.is_empty() && s.len() <= 256 && !s.contains('\0'))
    {
        return Err(Invalid);
    }
    if ordinal != 2
        && !input
            .attributes
            .as_object()
            .is_some_and(serde_json::Map::is_empty)
    {
        return Err(Unsupported);
    }
    let native = object
        .get("nativeRange")
        .and_then(Value::as_object)
        .ok_or(Invalid)?;
    let from = native.get("from").and_then(Value::as_u64).ok_or(Invalid)?;
    let to = native.get("to").and_then(Value::as_u64).ok_or(Invalid)?;
    if native.len() != 2 || from > to || to > SAFE {
        return Err(Invalid);
    }
    Ok(Geometry {
        source: *source_range,
        from,
        to,
    })
}

fn byte_offset(text: &str, target: u64) -> Result<usize> {
    let mut units = 0;
    for (offset, ch) in text.char_indices() {
        if units == target {
            return Ok(offset);
        }
        units += u64::try_from(ch.len_utf16()).map_err(|_| Invalid)?;
        if units > target {
            return Err(Invalid);
        }
    }
    if units == target {
        Ok(text.len())
    } else {
        Err(Invalid)
    }
}

/// The caller must supply exact immutable frozen bytes and digest-verified owned
/// resources, plus an independently validated original-root/scoped marker witness.
/// Success here proves only narrow shape/geometry/copy semantics, not authority.
/// The four descriptors cover the WHOLE paragraph, independently of selection.
/// # Errors
/// Invalid geometry/literal/attrs, unsupported schema/text, or paragraph budget.
pub fn marker_selection_markdown(
    input: &NoteSelectionMarkdownInput<'_>,
) -> Result<NoteMarkerSelectionMarkdown> {
    input.header.validate().map_err(|_| Invalid)?;
    if input.header.action != NoteStageAction::Read
        || input.header.output != NoteStageOutput::SelectionMarkdown
        || input.header.selection != NoteStageSelection::Ranges
        || input.selection.len() != 1
        || input.live.len() != 4
    {
        return Err(Unsupported);
    }
    let NoteStageRecord::Range {
        ordinal,
        start,
        end,
        direction,
        anchor_affinity,
        head_affinity,
    } = &input.selection[0]
    else {
        return Err(Invalid);
    };
    let frozen = input.frozen_range;
    if input.view_length > SAFE
        || frozen.start > frozen.end
        || frozen.end > input.view_length
        || *ordinal != 0
        || start > end
        || *start < frozen.start
        || *end > frozen.end
    {
        return Err(Invalid);
    }
    // All scans and allocations (including marker_literal's small strings) follow
    // these whole-paragraph limits; no source-wide hydration is authorized here.
    if input.frozen_paragraph.len() > 16384 || frozen.end - frozen.start > 4096 {
        return Err(Budget);
    }
    if u64::try_from(input.frozen_paragraph.encode_utf16().count()).map_err(|_| Invalid)?
        != frozen.end - frozen.start
    {
        return Err(Invalid);
    }
    let p = geometry(
        &input.live[0],
        0,
        NoteStageRole::SelectionOwner,
        "paragraph",
    )?;
    let left = geometry(&input.live[1], 1, NoteStageRole::InlineSpan, "text")?;
    let marker = geometry(
        &input.live[2],
        2,
        NoteStageRole::MarkerOccurrence,
        "commentAnchor",
    )?;
    let right = geometry(&input.live[3], 3, NoteStageRole::InlineSpan, "text")?;
    if p.source.start != frozen.start
        || p.source.end != frozen.end
        || left.source.start != frozen.start
        || left.source.end != marker.source.start
        || marker.source.end != right.source.start
        || right.source.end != frozen.end
        || left.source.start == left.source.end
        || right.source.start == right.source.end
        || marker.source.start == marker.source.end
    {
        return Err(Invalid);
    }
    if p.to > 32768 {
        return Err(Budget);
    }
    let left_units = left.source.end - left.source.start;
    let right_units = right.source.end - right.source.start;
    if left.from != p.from + 1
        || left.to - left.from != left_units
        || marker.from != left.to
        || marker.to != marker.from + 1
        || right.from != marker.to
        || right.to - right.from != right_units
        || p.to != right.to + 1
    {
        return Err(Invalid);
    }
    if [*start, *end]
        .into_iter()
        .any(|offset| marker.source.start < offset && offset < marker.source.end)
    {
        return Err(Invalid);
    }
    let a = byte_offset(input.frozen_paragraph, marker.source.start - frozen.start)?;
    let b = byte_offset(input.frozen_paragraph, marker.source.end - frozen.start)?;
    let left_text = &input.frozen_paragraph[..a];
    let literal = &input.frozen_paragraph[a..b];
    let right_text = &input.frozen_paragraph[b..];
    for text in [left_text, right_text] {
        if text.contains("  ")
            || !text
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b' ')
        {
            return Err(Unsupported);
        }
    }
    let marker_kind = marker_literal(&ResolvedMarkerInput {
        record: input.live[2].record,
        descriptor: input.live[2].descriptor,
        attributes: Some(input.live[2].attributes),
        frozen_literal: literal,
        frozen_range: marker.source,
        view_length: input.view_length,
    })
    .map_err(|error| match error {
        MarkerError::Invalid => Invalid,
        MarkerError::Unsupported => Unsupported,
    })?;
    if marker_kind != MarkerKind::Point {
        return Err(Unsupported);
    }
    let mut selected = String::new();
    for (text, range) in [(left_text, left.source), (right_text, right.source)] {
        let from = (*start).max(range.start);
        let to = (*end).min(range.end);
        if from < to {
            let a = usize::try_from(from - range.start).map_err(|_| Invalid)?;
            let b = usize::try_from(to - range.start).map_err(|_| Invalid)?;
            selected.push_str(text.get(a..b).ok_or(Invalid)?);
        }
    }
    let trimmed = selected.trim_matches(' ');
    Ok(NoteMarkerSelectionMarkdown {
        text: if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        },
        source_range: NoteStageRange {
            start: *start,
            end: *end,
        },
        direction: *direction,
        anchor_affinity: *anchor_affinity,
        head_affinity: *head_affinity,
    })
}

#[cfg(test)]
#[path = "note_stage_marker_selection_tests.rs"]
mod tests;
