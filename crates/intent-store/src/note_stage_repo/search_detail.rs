//! Raw source-hit fields, not lexical context or editor/native authority.
//!
//! The caller admits one sealed read snapshot, authenticates current visibility,
//! and derives `digest` from the original principal, scope, operation, query,
//! header/payload/view identities and exact original expiry. It checks liveness
//! again before publication. This helper never extends that lifetime or admits
//! arbitrary client ranges: only the search matcher may issue initial refs.
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use intent_core::{note_mutation::NoteMutationError, note_page::NotePageError, Error, Result};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::SqliteConnection;

const SAFE: u64 = 9_007_199_254_740_991;
const PREFIX: &str = "nsd1.";
const DOMAIN: &[u8] = b"intent.note.stage.source-hit.detail.v1\0";
const PAYLOAD_BYTES: usize = 56;
const TOKEN_BYTES: usize = PAYLOAD_BYTES + 32;
const TOKEN_CHARS: usize = 123;

#[derive(Clone, Copy)]
pub(super) struct SearchDetailBinding<'a> {
    pub operation: &'a str,
    pub generation: u64,
    pub view_length: u64,
    /// Caller-derived immutable identity digest; never a digest of the changing
    /// ref/offset request, and never a substitute for current authorization.
    pub digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SearchDetailPosition {
    pub start: u64,
    pub end: u64,
    pub offset: u64,
}

#[derive(Clone, Copy)]
pub(super) struct SearchDetailRead<'a> {
    pub key: &'a [u8],
    pub binding: &'a SearchDetailBinding<'a>,
    pub reference: &'a str,
    pub offset: Option<u64>,
    pub max_source_bytes: usize,
}

fn invalid() -> Error {
    Error::NotePage(NotePageError::CursorInvalid)
}
fn budget() -> Error {
    Error::NoteMutation(NoteMutationError::Budget)
}
fn binding_hash(binding: &SearchDetailBinding<'_>) -> Result<[u8; 32]> {
    if binding.operation.is_empty()
        || binding.operation.len() > 256
        || binding.operation.contains('\0')
        || binding.generation > SAFE
        || binding.view_length > SAFE
    {
        return Err(invalid());
    }
    let mut hash = Sha256::new();
    hash.update(DOMAIN);
    hash.update(binding.digest);
    hash.update(binding.generation.to_be_bytes());
    hash.update(binding.view_length.to_be_bytes());
    hash.update(binding.operation.as_bytes());
    Ok(hash.finalize().into())
}

fn check_position(binding: &SearchDetailBinding<'_>, at: SearchDetailPosition) -> Result<()> {
    if at.start >= at.end || at.end > binding.view_length || at.offset >= at.end - at.start {
        return Err(invalid());
    }
    Ok(())
}

fn encode_ref(
    key: &[u8],
    binding: &SearchDetailBinding<'_>,
    at: SearchDetailPosition,
) -> Result<String> {
    check_position(binding, at)?;
    // The backend persists a 256-bit signing key. Refuse accidentally missing
    // key material rather than turning it into a predictable valid MAC.
    if key.len() != 32 {
        return Err(invalid());
    }
    let mut bytes = Vec::with_capacity(TOKEN_BYTES);
    bytes.extend_from_slice(&at.start.to_be_bytes());
    bytes.extend_from_slice(&at.end.to_be_bytes());
    bytes.extend_from_slice(&at.offset.to_be_bytes());
    bytes.extend_from_slice(&binding_hash(binding)?);
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_| invalid())?;
    mac.update(DOMAIN);
    mac.update(&bytes);
    bytes.extend_from_slice(&mac.finalize().into_bytes());
    Ok(format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(bytes)))
}

/// Issue only for an actual nonempty hit of the admitted captured search.
/// Scalar endpoint validation occurs during resolution; issuing a signed range
/// is an internal authority operation, never a public arbitrary-range API.
pub(super) fn issue_ref(
    key: &[u8],
    binding: &SearchDetailBinding<'_>,
    start: u64,
    end: u64,
) -> Result<String> {
    encode_ref(
        key,
        binding,
        SearchDetailPosition {
            start,
            end,
            offset: 0,
        },
    )
}

pub(super) fn decode_ref(
    key: &[u8],
    binding: &SearchDetailBinding<'_>,
    reference: &str,
) -> Result<SearchDetailPosition> {
    if key.len() != 32 || reference.len() != TOKEN_CHARS {
        return Err(invalid());
    }
    let encoded = reference.strip_prefix(PREFIX).ok_or_else(invalid)?;
    let bytes = URL_SAFE_NO_PAD.decode(encoded).map_err(|_| invalid())?;
    if bytes.len() != TOKEN_BYTES || URL_SAFE_NO_PAD.encode(&bytes) != encoded {
        return Err(invalid());
    }
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_| invalid())?;
    mac.update(DOMAIN);
    mac.update(&bytes[..PAYLOAD_BYTES]);
    mac.verify_slice(&bytes[PAYLOAD_BYTES..])
        .map_err(|_| invalid())?;
    if bytes[24..PAYLOAD_BYTES] != binding_hash(binding)? {
        return Err(invalid());
    }
    let read = |start: usize| -> Result<u64> {
        Ok(u64::from_be_bytes(
            bytes[start..start + 8].try_into().map_err(|_| invalid())?,
        ))
    };
    let at = SearchDetailPosition {
        start: read(0)?,
        end: read(8)?,
        offset: read(16)?,
    };
    check_position(binding, at)?;
    Ok(at)
}

fn span_id(binding: &SearchDetailBinding<'_>, at: SearchDetailPosition) -> Result<String> {
    let mut hash = Sha256::new();
    hash.update(b"source-hit-field\0");
    hash.update(binding_hash(binding)?);
    hash.update(at.start.to_be_bytes());
    hash.update(at.end.to_be_bytes());
    Ok(format!("nsdi1.{}", URL_SAFE_NO_PAD.encode(hash.finalize())))
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
/// reconstruction, lexical parsing or full-source scan occurs.
pub(super) async fn read_fragment(
    conn: &mut SqliteConnection,
    request: &SearchDetailRead<'_>,
    mut fits: impl FnMut(&Value) -> Result<bool>,
) -> Result<Value> {
    if !(4..=16384).contains(&request.max_source_bytes) {
        return Err(budget());
    }
    let binding = request.binding;
    let at = decode_ref(request.key, binding, request.reference)?;
    if request.offset.is_some_and(|offset| offset != at.offset) {
        return Err(invalid());
    }
    // An issuer bug must not make half-surrogate spans readable. Even late
    // refs validate the original start/end, rather than only this page's start.
    for offset in [at.start, at.end] {
        super::view_read::read_piece(
            conn,
            binding.operation,
            binding.generation,
            binding.view_length,
            offset,
            4,
        )
        .await?;
    }
    let absolute = at.start.checked_add(at.offset).ok_or_else(invalid)?;
    let (read_end, mut text) = super::view_read::read_piece(
        conn,
        binding.operation,
        binding.generation,
        binding.view_length,
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
    let id = span_id(binding, at)?;
    loop {
        if text.is_empty() {
            return Err(budget());
        }
        let next = at.offset + u64::try_from(text.encode_utf16().count()).map_err(|_| invalid())?;
        let next_ref = if next == at.end - at.start {
            Value::Null
        } else {
            json!(encode_ref(
                request.key,
                binding,
                SearchDetailPosition { offset: next, ..at }
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
