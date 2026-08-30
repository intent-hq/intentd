//! Heavy-payload extraction for `agent_message` (0107, intent-hq/intent#3884).
//!
//! Multi-MB `tool_result.output` / `tool_use.input` bodies used to ride the
//! `agent_message.content` JSON column. The write path now extracts any such
//! body larger than [`PAYLOAD_INLINE_MAX_BYTES`] into the
//! `agent_message_payload` side table (zlib-compressed when that is smaller)
//! and leaves a `null` placeholder in the field's position — in-place, so key
//! order and every other byte of the content JSON are unchanged. Read paths
//! splice the bodies back before the content leaves the store, so wire shapes
//! are identical to pre-0107 behavior. Legacy rows (inline bodies, no side
//! rows) hydrate as no-ops: splicing is driven purely by side-row presence.
//!
//! Write-time image thumbnail maps (0097) also land here as message-level
//! rows (`kind = 'thumbnails'`, `block_ordinal` -1) instead of growing the
//! `agent_message.thumbnails` column; reads fall back to the legacy column.

use std::io::Write as _;

use intent_core::{slim_body_size, Error, Result};
use serde_json::Value;

/// Ceiling for a `tool_use.input` / `tool_result.output` body to stay inline
/// in `agent_message.content`. Bodies at or under this size are left alone
/// (no side row, no join cost); larger ones are externalized. Sized well
/// above [`intent_core::SLIM_PROJECTION_BUDGET_BYTES`] so the slim projection
/// still previews inline bodies without touching the side table.
pub(crate) const PAYLOAD_INLINE_MAX_BYTES: usize = 4096;

/// `kind` for an externalized `tool_use.input` body.
pub(crate) const KIND_TOOL_USE_INPUT: &str = "tool_use_input";
/// `kind` for an externalized `tool_result.output` body.
pub(crate) const KIND_TOOL_RESULT_OUTPUT: &str = "tool_result_output";
/// `kind` for a message-level write-time thumbnails map (0097 successor).
pub(crate) const KIND_THUMBNAILS: &str = "thumbnails";
/// `block_ordinal` for message-level rows (the thumbnails map is keyed by
/// image ordinal internally, not tied to one content block).
pub(crate) const THUMBNAILS_ORDINAL: i64 = -1;

/// `encoding` marker: raw serialized JSON bytes.
pub(crate) const ENCODING_NONE: &str = "none";
/// `encoding` marker: zlib-compressed serialized JSON bytes.
pub(crate) const ENCODING_ZLIB: &str = "zlib";

/// One row bound for insertion into `agent_message_payload`.
#[derive(Debug)]
pub(crate) struct PayloadRow {
    pub block_ordinal: i64,
    pub kind: &'static str,
    pub encoding: &'static str,
    pub body: Vec<u8>,
}

/// The externalizable field of a content block, by block type.
fn heavy_field(block_type: &str) -> Option<(&'static str, &'static str)> {
    match block_type {
        "tool_use" => Some(("input", KIND_TOOL_USE_INPUT)),
        "tool_result" => Some(("output", KIND_TOOL_RESULT_OUTPUT)),
        _ => None,
    }
}

/// Cheap write-path predicate: does `content` carry a block whose heavy field
/// exceeds [`PAYLOAD_INLINE_MAX_BYTES`]? Lets callers skip the content clone
/// for the common all-small message.
pub(crate) fn needs_extraction(content: &Value) -> bool {
    content.as_array().is_some_and(|blocks| {
        blocks.iter().any(|b| {
            b.get("type")
                .and_then(Value::as_str)
                .and_then(heavy_field)
                .and_then(|(field, _)| b.get(field))
                .is_some_and(|v| !v.is_null() && slim_body_size(v) > PAYLOAD_INLINE_MAX_BYTES)
        })
    })
}

/// Split `content` into its slim inline form plus the extracted side-table
/// rows. Returns `None` when nothing crosses the threshold (the common case —
/// callers then persist the original content untouched). Each externalized
/// field is replaced by `null` IN PLACE, preserving key order, so a hydrated
/// read re-serializes to the exact pre-extraction bytes.
///
/// # Errors
///
/// Returns `Error::Internal` if serializing an extracted body fails.
pub(crate) fn extract_payloads(content: &Value) -> Result<Option<(Value, Vec<PayloadRow>)>> {
    if !needs_extraction(content) {
        return Ok(None);
    }
    let mut slim = content.clone();
    let mut rows = Vec::new();
    if let Some(blocks) = slim.as_array_mut() {
        for (ordinal, block) in blocks.iter_mut().enumerate() {
            let Some((field, kind)) = block
                .get("type")
                .and_then(Value::as_str)
                .and_then(heavy_field)
            else {
                continue;
            };
            let Some(body) = block.get_mut(field) else {
                continue;
            };
            if body.is_null() || slim_body_size(body) <= PAYLOAD_INLINE_MAX_BYTES {
                continue;
            }
            let taken = std::mem::replace(body, Value::Null);
            let json = serde_json::to_vec(&taken).map_err(|e| {
                Error::Internal(format!("encode extracted payload body failed: {e}"))
            })?;
            let (encoding, stored) = encode_body(&json);
            rows.push(PayloadRow {
                block_ordinal: i64::try_from(ordinal).unwrap_or(i64::MAX),
                kind,
                encoding,
                body: stored,
            });
        }
    }
    Ok(Some((slim, rows)))
}

/// Compress `json` with zlib when that is smaller; otherwise store raw.
pub(crate) fn encode_body(json: &[u8]) -> (&'static str, Vec<u8>) {
    let mut enc = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    let compressed = enc
        .write_all(json)
        .and_then(|()| enc.finish())
        .unwrap_or_default();
    if !compressed.is_empty() && compressed.len() < json.len() {
        (ENCODING_ZLIB, compressed)
    } else {
        (ENCODING_NONE, json.to_vec())
    }
}

/// Decode a stored body back to its JSON value.
///
/// # Errors
///
/// Returns `Error::Internal` on an unknown encoding, corrupt zlib stream, or
/// unparseable JSON.
pub(crate) fn decode_body(encoding: &str, body: &[u8]) -> Result<Value> {
    let raw: Vec<u8> = match encoding {
        ENCODING_NONE => body.to_vec(),
        ENCODING_ZLIB => {
            let mut dec = flate2::write::ZlibDecoder::new(Vec::new());
            dec.write_all(body)
                .and_then(|()| dec.finish())
                .map_err(|e| Error::Internal(format!("decompress payload body failed: {e}")))?
        }
        other => {
            return Err(Error::Internal(format!(
                "unknown payload encoding '{other}'"
            )));
        }
    };
    serde_json::from_slice(&raw)
        .map_err(|e| Error::Internal(format!("decode payload body failed: {e}")))
}

/// Splice one decoded side-table body back into `content` at
/// `block_ordinal`'s heavy field — the write-time extraction in reverse. The
/// placeholder is overwritten in place, so key order is preserved. A row that
/// no longer lines up with its block (ordinal out of range, block type
/// mismatch) is skipped with a WARN — reads degrade to the `null` placeholder
/// rather than failing the whole transcript.
pub(crate) fn splice_payload(content: &mut Value, block_ordinal: i64, kind: &str, body: Value) {
    let expected = match kind {
        KIND_TOOL_USE_INPUT => ("tool_use", "input"),
        KIND_TOOL_RESULT_OUTPUT => ("tool_result", "output"),
        _ => {
            tracing::warn!(kind, "unknown payload kind; serving null placeholder");
            return;
        }
    };
    let block = usize::try_from(block_ordinal)
        .ok()
        .and_then(|i| content.as_array_mut()?.get_mut(i));
    match block {
        Some(b) if b.get("type").and_then(Value::as_str) == Some(expected.0) => {
            if let Some(obj) = b.as_object_mut() {
                obj.insert(expected.1.to_string(), body);
            }
        }
        _ => {
            tracing::warn!(
                block_ordinal,
                kind,
                "payload row does not match its content block; serving null placeholder"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn big_string() -> String {
        "x".repeat(PAYLOAD_INLINE_MAX_BYTES * 4)
    }

    #[test]
    fn small_bodies_stay_inline() {
        let content = json!([
            {"type": "text", "text": "hi"},
            {"type": "tool_use", "id": "t1", "name": "bash", "input": {"cmd": "ls"}},
            {"type": "tool_result", "toolCallId": "t1", "output": "ok"},
        ]);
        assert!(!needs_extraction(&content));
        assert!(extract_payloads(&content).unwrap().is_none());
    }

    #[test]
    fn oversized_bodies_extract_and_splice_back_identically() {
        let content = json!([
            {"type": "tool_use", "id": "t1", "name": "bash", "input": {"cmd": big_string()}},
            {"type": "text", "text": "between"},
            {"type": "tool_result", "toolCallId": "t1", "output": big_string()},
        ]);
        assert!(needs_extraction(&content));
        let (mut slim, rows) = extract_payloads(&content).unwrap().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].block_ordinal, 0);
        assert_eq!(rows[0].kind, KIND_TOOL_USE_INPUT);
        assert_eq!(rows[1].block_ordinal, 2);
        assert_eq!(rows[1].kind, KIND_TOOL_RESULT_OUTPUT);
        assert!(slim[0]["input"].is_null());
        assert!(slim[2]["output"].is_null());
        // Repetitive bodies compress.
        assert_eq!(rows[0].encoding, ENCODING_ZLIB);
        for row in rows {
            let body = decode_body(row.encoding, &row.body).unwrap();
            splice_payload(&mut slim, row.block_ordinal, row.kind, body);
        }
        assert_eq!(slim, content);
        // Placeholder-in-place: the re-serialized bytes match exactly.
        assert_eq!(
            serde_json::to_string(&slim).unwrap(),
            serde_json::to_string(&content).unwrap()
        );
    }

    #[test]
    fn incompressible_body_stores_raw_with_encoding_none() {
        // High-entropy bytes: zlib output is larger, so the raw JSON stays.
        let noise: String = (0..u32::try_from(PAYLOAD_INLINE_MAX_BYTES * 2).unwrap())
            .map(|i| {
                char::from_u32(0x4E00 + (i.wrapping_mul(2_654_435_761) % 20000)).unwrap_or('x')
            })
            .collect();
        let json_bytes = serde_json::to_vec(&Value::String(noise.clone())).unwrap();
        let (encoding, stored) = encode_body(&json_bytes);
        let decoded = decode_body(encoding, &stored).unwrap();
        assert_eq!(decoded, Value::String(noise));
    }

    #[test]
    fn encoding_none_roundtrip_and_unknown_encoding_errors() {
        let body = json!({"k": "v"});
        let raw = serde_json::to_vec(&body).unwrap();
        assert_eq!(decode_body(ENCODING_NONE, &raw).unwrap(), body);
        assert!(decode_body("gzip", &raw).is_err());
        assert!(decode_body(ENCODING_ZLIB, b"not zlib").is_err());
    }

    #[test]
    fn splice_mismatch_degrades_to_placeholder() {
        let mut content = json!([{"type": "text", "text": "hi"}]);
        let before = content.clone();
        splice_payload(&mut content, 0, KIND_TOOL_USE_INPUT, json!({"a": 1}));
        splice_payload(&mut content, 9, KIND_TOOL_RESULT_OUTPUT, json!("x"));
        splice_payload(&mut content, 0, "bogus", json!("x"));
        assert_eq!(content, before);
    }

    #[test]
    fn null_heavy_field_is_not_extracted() {
        let content = json!([
            {"type": "tool_result", "toolCallId": "t1", "output": null},
        ]);
        assert!(!needs_extraction(&content));
    }
}
