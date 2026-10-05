//! Pure, deliberately narrow native-selection copy semantics. This module does
//! not authenticate uploaded descriptors, acquire source, or publish an output.
use crate::note_stage::{
    NoteStageAction, NoteStageAffinity, NoteStageDirection, NoteStageHeader, NoteStageOutput,
    NoteStageRange, NoteStageRecord, NoteStageRole, NoteStageSelection,
};
use serde_json::Value;

const SAFE: u64 = 9_007_199_254_740_991;
/// Explicit subset caps, not support for arbitrarily large selections.
pub const PARAGRAPH_UNITS: usize = 4096;
const PARAGRAPH_BYTES: usize = 16384;
const NATIVE_WINDOW_UNITS: u64 = 32768;

/// Resolved resources must belong to this exact immutable operation/view and
/// match their uploaded references/digests. A borrowed Value is not that proof.
pub struct ResolvedSelectionDescriptor<'a> {
    pub record: &'a NoteStageRecord,
    pub descriptor: &'a Value,
    pub attributes: &'a Value,
}

/// The Store owns authorization, original expiry, generation/header binding,
/// exact stream closure, and admission BEFORE reading these bounded resources.
/// `frozen_paragraph` must be the exact frozen-view bytes at `frozen_range`, not
/// caller-uploaded replacement text or an inferred native/source coordinate map.
pub struct NoteSelectionMarkdownInput<'a> {
    pub header: &'a NoteStageHeader,
    pub selection: &'a [NoteStageRecord],
    pub live: &'a [ResolvedSelectionDescriptor<'a>],
    pub frozen_paragraph: &'a str,
    pub frozen_range: NoteStageRange,
    pub view_length: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SelectionMarkdownError {
    Invalid,
    Unsupported,
    Budget,
}

/// `None` means internal no-copy (collapsed/space-only), NOT an empty successful
/// clipboard write. The owner must define its no-publication path explicitly.
#[derive(Debug)]
pub struct NoteSelectionMarkdown<'a> {
    pub text: Option<&'a str>,
    pub source_range: NoteStageRange,
    pub direction: NoteStageDirection,
    pub anchor_affinity: NoteStageAffinity,
    pub head_affinity: NoteStageAffinity,
}

type Result<T> = std::result::Result<T, SelectionMarkdownError>;
use SelectionMarkdownError::{Budget, Invalid, Unsupported};

struct Descriptor {
    source: NoteStageRange,
    from: u64,
    to: u64,
}
fn descriptor(
    input: &ResolvedSelectionDescriptor<'_>,
    ordinal: u64,
    role: NoteStageRole,
    node: &str,
    parent: Option<u64>,
) -> Result<Descriptor> {
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
    if *actual_role != role || canonical_id.is_some() {
        return Err(Unsupported);
    }
    let object = input.descriptor.as_object().ok_or(Invalid)?;
    // A required explicit attributesRef excludes missing/default attrs. Exactly
    // these five fields comprise the supported version-1 descriptor subset.
    if object.len() != 5
        || object.get("version").and_then(Value::as_u64) != Some(1)
        || object.get("nodeType").and_then(Value::as_str) != Some(node)
    {
        return Err(Unsupported);
    }
    match (object.get("parentOrdinal"), parent) {
        (Some(Value::Null), None) => (),
        (Some(value), Some(expected)) if value.as_u64() == Some(expected) => (),
        _ => return Err(Unsupported),
    }
    let reference = object
        .get("attributesRef")
        .and_then(Value::as_str)
        .ok_or(Unsupported)?;
    if reference.is_empty() || reference.len() > 256 || reference.contains('\0') {
        return Err(Invalid);
    }
    if !input
        .attributes
        .as_object()
        .is_some_and(serde_json::Map::is_empty)
    {
        return Err(Unsupported);
    }
    let range = object
        .get("nativeRange")
        .and_then(Value::as_object)
        .ok_or(Invalid)?;
    if range.len() != 2 {
        return Err(Invalid);
    }
    let from = range.get("from").and_then(Value::as_u64).ok_or(Invalid)?;
    let to = range.get("to").and_then(Value::as_u64).ok_or(Invalid)?;
    if from > to || to > SAFE {
        return Err(Invalid);
    }
    Ok(Descriptor {
        source: *source_range,
        from,
        to,
    })
}

/// Serialize the positively captured one-paragraph subset: unmarked text with
/// explicit empty attrs, ASCII letters/digits/single spaces, and one native `TextSelection`.
/// Unsupported punctuation/Unicode/wrappers/marks never fall back to source echo.
/// Resident borrowed inputs are capped here; the owner still admits their IO,
/// parsed-resource storage and output lifetime. No source-wide scan or allocation.
///
/// # Errors
/// Rejects malformed ranges, unsupported context/text, or subset budget excess.
pub fn selection_markdown<'a>(
    input: &'a NoteSelectionMarkdownInput<'_>,
) -> Result<NoteSelectionMarkdown<'a>> {
    input.header.validate().map_err(|_| Invalid)?;
    if input.header.action != NoteStageAction::Read
        || input.header.output != NoteStageOutput::SelectionMarkdown
        || input.header.selection != NoteStageSelection::Ranges
        || input.selection.len() != 1
        || input.live.len() != 2
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
    if *ordinal != 0 || start > end || *end > input.view_length || input.view_length > SAFE {
        return Err(Invalid);
    }
    let paragraph = descriptor(
        &input.live[0],
        0,
        NoteStageRole::SelectionOwner,
        "paragraph",
        None,
    )?;
    let inline = descriptor(
        &input.live[1],
        1,
        NoteStageRole::InlineSpan,
        "text",
        Some(0),
    )?;
    if paragraph.source.start != input.frozen_range.start
        || paragraph.source.end != input.frozen_range.end
        || paragraph.source.end > input.view_length
        || *start < paragraph.source.start
        || *end > paragraph.source.end
        || inline.source.start != *start
        || inline.source.end != *end
    {
        return Err(Invalid);
    }
    if input.frozen_paragraph.len() > PARAGRAPH_BYTES
        || paragraph.source.end - paragraph.source.start > PARAGRAPH_UNITS as u64
        || paragraph.to > NATIVE_WINDOW_UNITS
    {
        return Err(Budget);
    }
    // This explicit initial grammar matches actual configured native-copy
    // controls (abc near/far, one two edges). Other Unicode is not admitted by
    // host lowercase/category assumptions or a generic Markdown escaping rule.
    if input.frozen_paragraph.is_empty()
        || input.frozen_paragraph.contains("  ")
        || !input
            .frozen_paragraph
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b' ')
    {
        return Err(Unsupported);
    }
    let length = input.frozen_paragraph.len() as u64; // every admitted scalar is one UTF16 unit
    if length != paragraph.source.end - paragraph.source.start
        || paragraph.to - paragraph.from != length + 2
        || inline.from != paragraph.from + 1 + start - paragraph.source.start
        || inline.to != paragraph.from + 1 + end - paragraph.source.start
    {
        return Err(Invalid);
    }
    let selected = input
        .frozen_paragraph
        .get(
            usize::try_from(start - paragraph.source.start).map_err(|_| Invalid)?
                ..usize::try_from(end - paragraph.source.start).map_err(|_| Invalid)?,
        )
        .ok_or(Invalid)?;
    let text = selected.trim_matches(' ');
    Ok(NoteSelectionMarkdown {
        text: if text.is_empty() { None } else { Some(text) },
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
#[path = "note_stage_selection_markdown/tests.rs"]
mod tests;
