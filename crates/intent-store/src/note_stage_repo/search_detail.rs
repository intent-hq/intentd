//! Raw source-hit fields, not lexical context or editor/native authority.
//!
//! The caller admits one sealed read snapshot, authenticates current visibility,
//! and derives `Context.binding` from the original principal, scope, operation, query,
//! header/payload/view identities and exact original expiry. It checks liveness
//! again before publication. This helper never extends that lifetime or admits
//! arbitrary client ranges: only the search matcher may issue initial refs.
use super::search_output::{hit_id, invalid, sign, verify, Context};
use intent_core::{note_mutation::NoteMutationError, Error, Result};
use serde_json::{json, Value};
use sqlx::SqliteConnection;

const SAFE: u64 = 9_007_199_254_740_991;
const PREFIX: &str = "nsh1.";
const TOKEN_CHARS: usize = 123;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SearchDetailPosition {
    pub start: u64,
    pub end: u64,
    pub offset: u64,
}

#[derive(Clone, Copy)]
pub(super) struct SearchDetailRead<'a> {
    pub context: &'a Context,
    pub reference: &'a str,
    pub offset: Option<u64>,
    pub max_source_bytes: usize,
}

fn budget() -> Error {
    Error::NoteMutation(NoteMutationError::Budget)
}

pub(super) fn decode_ref(context: &Context, reference: &str) -> Result<SearchDetailPosition> {
    if reference.len() != TOKEN_CHARS || context.length > SAFE || context.generation > SAFE {
        return Err(invalid());
    }
    let values = verify(reference, PREFIX, &context.key, &context.binding, 3)?;
    let [start, end, offset]: [u64; 3] = values.try_into().map_err(|_| invalid())?;
    if start >= end || end > context.length || offset >= end - start {
        return Err(invalid());
    }
    // Exact re-encoding rejects alternate base64 spellings. Initial minting is
    // owned exclusively by search_output after accepting an actual source hit.
    if sign(
        PREFIX,
        &context.key,
        &context.binding,
        &[start, end, offset],
    )? != reference
    {
        return Err(invalid());
    }
    Ok(SearchDetailPosition { start, end, offset })
}

fn smaller_prefix(text: &str) -> usize {
    let first = text.chars().next().map_or(0, char::len_utf8);
    if text.len() <= first {
        return 0;
    }
    let mut bytes = (text.len() / 2).max(first);
    while !text.is_char_boundary(bytes) {
        bytes -= 1;
    }
    bytes
}

/// Return one nonempty exact raw fragment. `fits` must measure the COMPLETE
/// escaped JSON-RPC frame containing this item and `nextCursor:null`, including
/// actual RPC id, original view envelope and signed nextRef. It must not publish.
///
/// The caller rejects collection cursors/textIds and routes only staged source
/// hit refs here. `offset`, when supplied, must equal the ref-bound position.
/// Resident source is <=16384 UTF8 bytes plus constant bounded view pieces.
/// Two bounded endpoint probes precede the indexed payload read; no match-wide
/// reconstruction, lexical parsing or full-source scan occurs. The source budget
/// bounds returned text, not unique backing-piece hydration: each endpoint probe
/// may load backing pieces too. The caller retains read admission through those
/// awaits and connection settlement; this helper starts no detached work.
pub(super) async fn read_fragment(
    conn: &mut SqliteConnection,
    request: &SearchDetailRead<'_>,
    mut fits: impl FnMut(&Value) -> Result<bool>,
) -> Result<Value> {
    if !(4..=16384).contains(&request.max_source_bytes) {
        return Err(budget());
    }
    let context = request.context;
    let at = decode_ref(context, request.reference)?;
    if request.offset.is_some_and(|offset| offset != at.offset) {
        return Err(invalid());
    }
    // An issuer bug must not make half-surrogate spans readable. Even late
    // refs validate the original start/end, rather than only this page's start.
    for offset in [at.start, at.end] {
        super::view_read::read_piece(
            conn,
            &context.operation,
            context.generation,
            context.length,
            offset,
            4,
        )
        .await?;
    }
    let absolute = at.start.checked_add(at.offset).ok_or_else(invalid)?;
    let (read_end, mut text) = super::view_read::read_piece(
        conn,
        &context.operation,
        context.generation,
        context.length,
        absolute,
        request.max_source_bytes,
    )
    .await?;
    let mut end = absolute;
    let mut truncate = text.len();
    for (byte, c) in text.char_indices() {
        if end == at.end {
            truncate = byte;
            break;
        }
        end += u64::try_from(c.len_utf16()).expect("scalar UTF16 width fits");
        if end > at.end {
            return Err(invalid());
        }
    }
    if end > read_end || end <= absolute {
        return Err(invalid());
    }
    text.truncate(truncate);
    let id = hit_id(&context.binding, at.start, at.end);
    loop {
        if text.is_empty() {
            return Err(budget());
        }
        let next = at.offset + u64::try_from(text.encode_utf16().count()).map_err(|_| invalid())?;
        let next_ref = if next == at.end - at.start {
            Value::Null
        } else {
            json!(sign(
                PREFIX,
                &context.key,
                &context.binding,
                &[at.start, at.end, next]
            )?)
        };
        let item = json!({"kind":"fragment","id":id,"field":"source","offset":at.offset,"text":text,"nextRef":next_ref});
        if fits(&item)? {
            return Ok(item);
        }
        let bytes = smaller_prefix(&text);
        text.truncate(bytes);
    }
}

#[cfg(test)]
#[path = "search_detail_tests.rs"]
mod tests;
