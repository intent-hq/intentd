//! Pure validation for the initial rendered-search identity capture. No IO,
//! authorization, native-schema capture or resource ownership is established here.
use crate::{
    note_stage::{
        NoteStageAction, NoteStageAffinity, NoteStageDirection, NoteStageHeader, NoteStageOutput,
        NoteStageRange, NoteStageRecord, NoteStageRole, NoteStageSearchMode, NoteStageSelection,
        NoteStageTextReference,
    },
    note_stage_selection_markdown::ResolvedSelectionDescriptor,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

const SAFE: u64 = 9_007_199_254_740_991;
/// Initial adapter subset caps, not a total operation/upload limit.
pub const RENDERED_UNITS: usize = 4096;
pub const RENDERED_BYTES: usize = 16384;
const NATIVE_WINDOW_UNITS: u64 = 32768;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RenderedError {
    Invalid,
    Unsupported,
    Budget,
}
type Result<T> = std::result::Result<T, RenderedError>;
use RenderedError::{Budget, Invalid, Unsupported};

/// Every descriptor/attribute/text resource must already be digest-verified and
/// owned by the same immutable operation/view. Store validates exact stream
/// closure, header/generation, original expiry and configured native capture.
/// Merely constructing this borrowed input is not evidence of those conditions.
pub struct NoteRenderedInput<'a, 'text> {
    pub header: &'a NoteStageHeader,
    pub selection: &'a [NoteStageRecord],
    pub live: &'a [ResolvedSelectionDescriptor<'a>],
    pub frozen_paragraph: &'a str,
    pub frozen_range: NoteStageRange,
    pub view_length: u64,
    pub rendered_reference: &'a NoteStageTextReference,
    pub rendered_text: &'text str,
}

/// Whole captured leaf, never trimmed or folded. The selected search domain is
/// leaf-relative; the caller's matcher retains its own folding/continuation state.
#[non_exhaustive]
#[derive(Debug)]
pub struct RenderedIdentity<'a> {
    pub text: &'a str,
    pub source_range: NoteStageRange,
    pub native_range: NoteStageRange,
    pub selected_range: NoteStageRange,
    pub leaf_ordinal: u64,
    pub direction: NoteStageDirection,
    pub anchor_affinity: NoteStageAffinity,
    pub head_affinity: NoteStageAffinity,
}
impl RenderedIdentity<'_> {
    /// Map a scalar-aligned, nonempty matcher hit inside the selected domain.
    /// # Errors
    /// Rejects empty, widened or non-scalar ranges; this does not mint hit IDs.
    pub fn map_hit(&self, start: u64, end: u64) -> Result<NoteStageRange> {
        if start >= end
            || start < self.selected_range.start
            || end > self.selected_range.end
            || !boundary(self.text, start)
            || !boundary(self.text, end)
        {
            return Err(Invalid);
        }
        let start = self.source_range.start.checked_add(start).ok_or(Invalid)?;
        let end = self.source_range.start.checked_add(end).ok_or(Invalid)?;
        if end > self.source_range.end || end > SAFE {
            return Err(Invalid);
        }
        Ok(NoteStageRange { start, end })
    }
}
fn boundary(text: &str, at: u64) -> bool {
    let mut units = 0;
    for scalar in text.chars() {
        if units == at {
            return true;
        }
        units += u64::try_from(scalar.len_utf16()).expect("scalar width fits");
        if units > at {
            return false;
        }
    }
    units == at
}
fn token(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.contains('\0')
}
fn same_range(a: NoteStageRange, b: NoteStageRange) -> bool {
    a.start == b.start && a.end == b.end
}
struct Geometry {
    source: NoteStageRange,
    from: u64,
    to: u64,
}
fn geometry(input: &ResolvedSelectionDescriptor<'_>, leaf: bool) -> Result<Geometry> {
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
    let (expected_ordinal, expected_role, node, version, fields) = if leaf {
        (1, NoteStageRole::InlineSpan, "text", 2, 6)
    } else {
        (0, NoteStageRole::SelectionOwner, "paragraph", 1, 5)
    };
    if *ordinal != expected_ordinal
        || source_range.start > source_range.end
        || source_range.end > SAFE
    {
        return Err(Invalid);
    }
    if *role != expected_role || canonical_id.is_some() {
        return Err(Unsupported);
    }
    let object = input.descriptor.as_object().ok_or(Invalid)?;
    if object.len() != fields
        || object.get("version").and_then(Value::as_u64) != Some(version)
        || object.get("nodeType").and_then(Value::as_str) != Some(node)
    {
        return Err(Unsupported);
    }
    match (object.get("parentOrdinal"), leaf) {
        (Some(Value::Null), false) => (),
        (Some(parent), true) if parent.as_u64() == Some(0) => (),
        _ => return Err(Unsupported),
    }
    let attrs = object
        .get("attributesRef")
        .and_then(Value::as_str)
        .ok_or(Unsupported)?;
    if !token(attrs) {
        return Err(Invalid);
    }
    if !input
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
    if native.len() != 2 {
        return Err(Invalid);
    }
    let from = native.get("from").and_then(Value::as_u64).ok_or(Invalid)?;
    let to = native.get("to").and_then(Value::as_u64).ok_or(Invalid)?;
    if from > to || to > SAFE {
        return Err(Invalid);
    }
    Ok(Geometry {
        source: *source_range,
        from,
        to,
    })
}
fn verify_rendered(input: &NoteRenderedInput<'_, '_>) -> Result<()> {
    let raw = input.live[1]
        .descriptor
        .get("renderedText")
        .ok_or(Unsupported)?;
    let object = raw.as_object().ok_or(Invalid)?;
    if object.len() != 4 {
        return Err(Invalid);
    }
    let text_id = object
        .get("textId")
        .and_then(Value::as_str)
        .ok_or(Invalid)?;
    let length = object
        .get("length")
        .and_then(Value::as_u64)
        .ok_or(Invalid)?;
    let utf8_bytes = object
        .get("utf8Bytes")
        .and_then(Value::as_u64)
        .ok_or(Invalid)?;
    let sha256 = object
        .get("sha256")
        .and_then(Value::as_str)
        .ok_or(Invalid)?;
    let supplied = input.rendered_reference;
    if !token(text_id)
        || length > SAFE
        || utf8_bytes > SAFE
        || sha256.len() != 64
        || !sha256
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        || text_id != supplied.text_id
        || length != supplied.length
        || utf8_bytes != supplied.utf8_bytes
        || sha256 != supplied.sha256
    {
        return Err(Invalid);
    }
    let text = input.rendered_text;
    if text.is_empty() || text.contains('\0') || text != input.frozen_paragraph {
        return Err(Invalid);
    }
    if u64::try_from(text.len()).map_err(|_| Budget)? != utf8_bytes
        || u64::try_from(text.encode_utf16().count()).map_err(|_| Budget)? != length
    {
        return Err(Invalid);
    }
    let hash = Sha256::digest(text.as_bytes());
    let hex = b"0123456789abcdef";
    if !hash.iter().enumerate().all(|(i, byte)| {
        sha256.as_bytes()[2 * i] == hex[usize::from(byte >> 4)]
            && sha256.as_bytes()[2 * i + 1] == hex[usize::from(byte & 15)]
    }) {
        return Err(Invalid);
    }
    Ok(())
}

/// Validate whole-leaf scalar identity and return the raw rendered search domain.
/// Spaces/Unicode are preserved; no selection-Markdown serialization is reused.
/// # Errors
/// Rejects unsupported context/header, malformed geometry/resource tuple and
/// subset budget excess. Equal lengths alone never establish identity.
pub fn rendered_identity<'text>(
    input: &NoteRenderedInput<'_, 'text>,
) -> Result<RenderedIdentity<'text>> {
    input.header.validate().map_err(|_| Invalid)?;
    // Rendered matching preserves whitespace literally; only empty is invalid.
    // The shared header currently applies its nonempty check to source mode.
    if input
        .header
        .query
        .as_ref()
        .is_some_and(|query| query.text.is_empty())
    {
        return Err(Invalid);
    }
    if input.header.action != NoteStageAction::Read
        || input.header.output != NoteStageOutput::Search
        || input.header.selection != NoteStageSelection::Ranges
        || !input
            .header
            .query
            .as_ref()
            .is_some_and(|query| query.mode == NoteStageSearchMode::RenderedText)
        || input.selection.len() != 1
        || input.live.len() != 2
    {
        return Err(Unsupported);
    }
    if input.view_length > SAFE {
        return Err(Invalid);
    }
    // Check all supplied source bytes before hashing/scanning/copying resources.
    if input.frozen_paragraph.len() > RENDERED_BYTES || input.rendered_text.len() > RENDERED_BYTES {
        return Err(Budget);
    }
    let parent = geometry(&input.live[0], false)?;
    let leaf = geometry(&input.live[1], true)?;
    if !same_range(parent.source, leaf.source)
        || !same_range(leaf.source, input.frozen_range)
        || leaf.source.end > input.view_length
    {
        return Err(Invalid);
    }
    let length = leaf.source.end - leaf.source.start;
    if length > u64::try_from(RENDERED_UNITS).map_err(|_| Budget)?
        || parent.to > NATIVE_WINDOW_UNITS
    {
        return Err(Budget);
    }
    if length == 0
        || parent.to - parent.from != length + 2
        || leaf.from != parent.from + 1
        || leaf.to + 1 != parent.to
        || leaf.to - leaf.from != length
    {
        return Err(Invalid);
    }
    verify_rendered(input)?;
    if input.rendered_reference.length != length {
        return Err(Invalid);
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
    if *ordinal != 0 || start > end || *start < leaf.source.start || *end > leaf.source.end {
        return Err(Invalid);
    }
    let selected_range = NoteStageRange {
        start: start - leaf.source.start,
        end: end - leaf.source.start,
    };
    if !boundary(input.rendered_text, selected_range.start)
        || !boundary(input.rendered_text, selected_range.end)
    {
        return Err(Invalid);
    }
    Ok(RenderedIdentity {
        text: input.rendered_text,
        source_range: leaf.source,
        native_range: NoteStageRange {
            start: leaf.from,
            end: leaf.to,
        },
        selected_range,
        leaf_ordinal: 1,
        direction: *direction,
        anchor_affinity: *anchor_affinity,
        head_affinity: *head_affinity,
    })
}

#[cfg(test)]
#[path = "note_stage_rendered/tests.rs"]
mod tests;
