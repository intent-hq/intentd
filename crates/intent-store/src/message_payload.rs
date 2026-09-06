//! Heavy-payload extraction for `agent_message` (0108, intent-hq/intent#3884).
//!
//! Multi-MB `tool_result.output` / `tool_use.input` bodies used to ride the
//! `agent_message.content` JSON column. The write path now extracts any such
//! body larger than [`PAYLOAD_INLINE_MAX_BYTES`] into the
//! `agent_message_payload` side table (zlib-compressed when that is smaller)
//! and leaves the slim-projection preview plus `*Truncated`/`*Bytes` flags in
//! the field's position — the SAME transform the serve-time slim projection
//! applies ([`intent_core::slim_heavy_body`], shared), so a slim page read
//! serves straight from the content column with NO side-table access.
//! Full-fidelity read paths splice the original body back (stripping the
//! flags) before the content leaves the store, so their wire shapes are
//! identical to pre-0108 behavior (`serde_json` maps serialize key-sorted, so
//! the flag round-trip is byte-invisible). Legacy rows (inline bodies, no
//! side rows) hydrate as no-ops: splicing is driven purely by side-row
//! presence.
//!
//! Write-time image thumbnail maps (0097) also land here as message-level
//! rows (`kind = 'thumbnails'`, `block_ordinal` -1) instead of growing the
//! `agent_message.thumbnails` column; reads fall back to the legacy column.
//!
//! The tool-payload retention sweep (`agents.toolPayloadRetentionDays`)
//! compacts an old full-body row into a `*_replay` row holding exactly the
//! replay-shaped preview ([`ReplayPreview`]) and deletes the full body.
//! Normal hydration ignores replay kinds — a pruned block keeps serving its
//! inline slim preview + flags — while the recovery-replay read
//! ([`splice_replay_preview`]) emits the `intent_core::replay_preview` block
//! contract from either a full row or a replay row, byte-identically.

use std::io::Write as _;

use intent_core::replay_preview::{
    retruncate_replay_preview, safe_stringify, truncate_marked, INPUT_REPLAY_ORIGINAL_CHARS_KEY,
    OUTPUT_REPLAY_ORIGINAL_CHARS_KEY,
};
use intent_core::{slim_body_size, slim_heavy_body, Error, Result};
use serde::{Deserialize, Serialize};
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
/// `kind` for the replay-shaped preview of a pruned `tool_use.input` body
/// (the full [`KIND_TOOL_USE_INPUT`] row is gone).
pub(crate) const KIND_TOOL_USE_INPUT_REPLAY: &str = "tool_use_input_replay";
/// `kind` for the replay-shaped preview of a pruned `tool_result.output` body
/// (the full [`KIND_TOOL_RESULT_OUTPUT`] row is gone).
pub(crate) const KIND_TOOL_RESULT_OUTPUT_REPLAY: &str = "tool_result_output_replay";
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

/// The externalizable field of a content block plus its slim-preview flags,
/// by block type. The flag names match the serve-time slim projection
/// (intent-services `tool_block`) — the write-time preview must be
/// indistinguishable from a serve-time one.
struct HeavyField {
    field: &'static str,
    kind: &'static str,
    replay_kind: &'static str,
    truncated_flag: &'static str,
    bytes_flag: &'static str,
    replay_marker: &'static str,
}

fn heavy_field(block_type: &str) -> Option<HeavyField> {
    match block_type {
        "tool_use" => Some(HeavyField {
            field: "input",
            kind: KIND_TOOL_USE_INPUT,
            replay_kind: KIND_TOOL_USE_INPUT_REPLAY,
            truncated_flag: "inputTruncated",
            bytes_flag: "inputBytes",
            replay_marker: INPUT_REPLAY_ORIGINAL_CHARS_KEY,
        }),
        "tool_result" => Some(HeavyField {
            field: "output",
            kind: KIND_TOOL_RESULT_OUTPUT,
            replay_kind: KIND_TOOL_RESULT_OUTPUT_REPLAY,
            truncated_flag: "outputTruncated",
            bytes_flag: "outputBytes",
            replay_marker: OUTPUT_REPLAY_ORIGINAL_CHARS_KEY,
        }),
        _ => None,
    }
}

/// The block type + field targeted by a full-body side-table row's `kind`.
fn kind_target(kind: &str) -> Option<(&'static str, HeavyField)> {
    match kind {
        KIND_TOOL_USE_INPUT => Some(("tool_use", heavy_field("tool_use")?)),
        KIND_TOOL_RESULT_OUTPUT => Some(("tool_result", heavy_field("tool_result")?)),
        _ => None,
    }
}

/// [`kind_target`] accepting a replay-preview `kind` as well as a full-body one.
fn replay_kind_target(kind: &str) -> Option<(&'static str, HeavyField)> {
    match kind {
        KIND_TOOL_USE_INPUT_REPLAY => Some(("tool_use", heavy_field("tool_use")?)),
        KIND_TOOL_RESULT_OUTPUT_REPLAY => Some(("tool_result", heavy_field("tool_result")?)),
        other => kind_target(other),
    }
}

/// The `*_replay` kind a full-body row's `kind` compacts into; `None` for
/// kinds the retention sweep never touches (thumbnails, already-replay rows).
pub(crate) fn replay_kind_for(kind: &str) -> Option<&'static str> {
    kind_target(kind).map(|(_, f)| f.replay_kind)
}

/// Whether `kind` names a replay-preview row (as opposed to a full body).
pub(crate) fn is_replay_kind(kind: &str) -> bool {
    kind == KIND_TOOL_USE_INPUT_REPLAY || kind == KIND_TOOL_RESULT_OUTPUT_REPLAY
}

/// For a replay-preview `kind`: the pruned field it stands for plus the
/// full-body `kind` it replaced (whose absence at the same ordinal proves
/// the prune). `None` for full-body and thumbnails kinds.
pub(crate) fn pruned_field_for(
    kind: &str,
) -> Option<(crate::agent_repo::PrunedToolField, &'static str)> {
    match kind {
        KIND_TOOL_USE_INPUT_REPLAY => Some((
            crate::agent_repo::PrunedToolField::ToolUseInput,
            KIND_TOOL_USE_INPUT,
        )),
        KIND_TOOL_RESULT_OUTPUT_REPLAY => Some((
            crate::agent_repo::PrunedToolField::ToolResultOutput,
            KIND_TOOL_RESULT_OUTPUT,
        )),
        _ => None,
    }
}

/// The decoded body of a `*_replay` row: the replay-shaped preview of a
/// pruned heavy body. Serialized as the JSON object
/// `{"text": <middle-truncated stringified body>, "originalChars": <N>}`
/// (then [`encode_body`] like every other row). `text` is what
/// `truncate_marked(safe_stringify(body), replay_chars)` produced at prune
/// time and `original_chars` is the full stringified body's char count — for
/// a body that already fit under the cap, `text` is the whole body and
/// `original_chars` its own char count (uniform shape; the formatter treats
/// `preview_chars >= original_chars` as "whole body present").
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ReplayPreview {
    pub text: String,
    pub original_chars: usize,
}

impl ReplayPreview {
    /// Produce the replay preview of a decoded full body at `replay_chars`.
    pub(crate) fn from_body(body: &Value, replay_chars: usize) -> Self {
        let (text, original) = truncate_marked(&safe_stringify(body), replay_chars);
        let original_chars = original.unwrap_or_else(|| text.chars().count());
        Self {
            text,
            original_chars,
        }
    }

    /// Parse a decoded `*_replay` row body.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` when the body is not a replay-preview object.
    pub(crate) fn from_row_body(body: Value) -> Result<Self> {
        serde_json::from_value(body)
            .map_err(|e| Error::Internal(format!("decode replay preview body failed: {e}")))
    }

    /// Serialize + [`encode_body`] for insertion as a `*_replay` row.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if serialization fails.
    pub(crate) fn encode(&self) -> Result<(&'static str, Vec<u8>)> {
        let json = serde_json::to_vec(self)
            .map_err(|e| Error::Internal(format!("encode replay preview body failed: {e}")))?;
        Ok(encode_body(&json))
    }
}

/// Splice a replay preview into `content` at `block_ordinal`'s heavy field,
/// emitting the `intent_core::replay_preview` block contract: the heavy field
/// becomes the preview STRING rendered at `replay_chars`
/// ([`retruncate_replay_preview`] — re-truncated when the cap shrank since the
/// preview was produced, never expanded) and the additive
/// `inputReplayOriginalChars` / `outputReplayOriginalChars` integer carries
/// the original char count; the write-time `*Truncated` / `*Bytes` flags are
/// removed like [`splice_payload`] does. `kind` may be the full-body or the
/// `*_replay` kind — both paths yield the same block for the same body and
/// cap. Mismatched rows degrade to the stored preview with a WARN.
pub(crate) fn splice_replay_preview(
    content: &mut Value,
    block_ordinal: i64,
    kind: &str,
    preview: &ReplayPreview,
    replay_chars: usize,
) {
    let Some((expected_type, f)) = replay_kind_target(kind) else {
        tracing::warn!(kind, "unknown payload kind; serving stored preview");
        return;
    };
    let block = usize::try_from(block_ordinal)
        .ok()
        .and_then(|i| content.as_array_mut()?.get_mut(i));
    match block {
        Some(b) if b.get("type").and_then(Value::as_str) == Some(expected_type) => {
            if let Some(obj) = b.as_object_mut() {
                let (text, original) =
                    retruncate_replay_preview(&preview.text, preview.original_chars, replay_chars);
                let original_chars = original.unwrap_or_else(|| text.chars().count());
                obj.insert(f.field.to_string(), Value::String(text));
                obj.insert(f.replay_marker.to_string(), Value::from(original_chars));
                obj.remove(f.truncated_flag);
                obj.remove(f.bytes_flag);
            }
        }
        _ => {
            tracing::warn!(
                block_ordinal,
                kind,
                "payload row does not match its content block; serving stored preview"
            );
        }
    }
}

/// Cheap write-path predicate: does `content` carry a block whose heavy field
/// exceeds [`PAYLOAD_INLINE_MAX_BYTES`]? Lets callers skip the content clone
/// for the common all-small message. Mirrors [`slim_heavy_body`]'s guards
/// exactly (already-flagged blocks pass through), so `true` here means
/// [`extract_payloads`] will produce at least one row.
pub(crate) fn needs_extraction(content: &Value) -> bool {
    content
        .as_array()
        .is_some_and(|blocks| blocks.iter().any(block_needs_extraction))
}

/// [`needs_extraction`] for a single content block: `true` iff
/// [`extract_block_payload`] would produce a row for it.
pub(crate) fn block_needs_extraction(block: &Value) -> bool {
    let Some(f) = block
        .get("type")
        .and_then(Value::as_str)
        .and_then(heavy_field)
    else {
        return false;
    };
    if block.get(f.truncated_flag).and_then(Value::as_bool) == Some(true) {
        return false;
    }
    block
        .get(f.field)
        .is_some_and(|v| slim_body_size(v) > PAYLOAD_INLINE_MAX_BYTES)
}

/// Split `content` into its slim inline form plus the extracted side-table
/// rows (empty when nothing crosses the threshold — the common case). Each
/// externalized field is replaced IN PLACE by the shared slim-projection
/// preview + `*Truncated`/`*Bytes` flags ([`slim_heavy_body`]), so a slim
/// page read serves the stored column as-is; [`splice_payload`] strips the
/// flags again, and content-JSON serialization is key-sorted, so a hydrated
/// read re-serializes to the exact pre-extraction bytes. Takes `content` by
/// value: the caller already cloned it onto the blocking thread, and this
/// consumes that clone instead of making a second multi-MB copy.
///
/// # Errors
///
/// Returns `Error::Internal` if serializing an extracted body fails.
pub(crate) fn extract_payloads(mut content: Value) -> Result<(Value, Vec<PayloadRow>)> {
    let mut rows = Vec::new();
    if let Some(blocks) = content.as_array_mut() {
        for (ordinal, block) in blocks.iter_mut().enumerate() {
            let (slim, row) = extract_block_payload(
                std::mem::take(block),
                i64::try_from(ordinal).unwrap_or(i64::MAX),
            )?;
            *block = slim;
            if let Some(row) = row {
                rows.push(row);
            }
        }
    }
    Ok((content, rows))
}

/// [`extract_payloads`] for one content block: returns the block back —
/// slimmed to its placeholder form when its heavy field crossed the
/// threshold, untouched otherwise — plus the extracted side-table row keyed
/// at `block_ordinal` (`None` when nothing was externalized). This is the
/// unit the mid-turn prestage path stages block-by-block as blocks complete
/// (intent-hq/intent#3884 part 2); the whole-message path above delegates
/// here so both produce byte-identical placeholders and rows.
///
/// # Errors
///
/// Returns `Error::Internal` if serializing the extracted body fails.
pub(crate) fn extract_block_payload(
    mut block: Value,
    block_ordinal: i64,
) -> Result<(Value, Option<PayloadRow>)> {
    let Some(f) = block
        .get("type")
        .and_then(Value::as_str)
        .and_then(heavy_field)
    else {
        return Ok((block, None));
    };
    let Some(body) = slim_heavy_body(
        &mut block,
        f.field,
        f.truncated_flag,
        f.bytes_flag,
        PAYLOAD_INLINE_MAX_BYTES,
    ) else {
        return Ok((block, None));
    };
    let json = serde_json::to_vec(&body)
        .map_err(|e| Error::Internal(format!("encode extracted payload body failed: {e}")))?;
    let (encoding, stored) = encode_body(&json);
    Ok((
        block,
        Some(PayloadRow {
            block_ordinal,
            kind: f.kind,
            encoding,
            body: stored,
        }),
    ))
}

/// The `(block_ordinal, kind)` side-table keys `content`'s placeholder
/// blocks reference: every block already carrying its `*Truncated` flag,
/// i.e. whose heavy body lives (or is expected to live) in
/// `agent_message_payload` rather than inline. The prestaged append path
/// uses these to reconcile staged rows against the final content — a staged
/// row not referenced by a placeholder (and not re-inserted) is stale and
/// must be deleted, or hydration would splice an outdated body over the
/// inline field.
pub(crate) fn placeholder_keys(content: &Value) -> Vec<(i64, &'static str)> {
    let Some(blocks) = content.as_array() else {
        return Vec::new();
    };
    blocks
        .iter()
        .enumerate()
        .filter_map(|(i, b)| {
            let f = b
                .get("type")
                .and_then(Value::as_str)
                .and_then(heavy_field)?;
            (b.get(f.truncated_flag).and_then(Value::as_bool) == Some(true))
                .then(|| (i64::try_from(i).unwrap_or(i64::MAX), f.kind))
        })
        .collect()
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
/// preview placeholder is overwritten and the write-time `*Truncated` /
/// `*Bytes` flags removed, restoring the pre-extraction block exactly
/// (content JSON serializes key-sorted, so the round-trip is
/// byte-invisible). A row that no longer lines up with its block (ordinal
/// out of range, block type mismatch) is skipped with a WARN — reads degrade
/// to the stored preview rather than failing the whole transcript.
pub(crate) fn splice_payload(content: &mut Value, block_ordinal: i64, kind: &str, body: Value) {
    let Some((expected_type, f)) = kind_target(kind) else {
        tracing::warn!(kind, "unknown payload kind; serving stored preview");
        return;
    };
    let block = usize::try_from(block_ordinal)
        .ok()
        .and_then(|i| content.as_array_mut()?.get_mut(i));
    match block {
        Some(b) if b.get("type").and_then(Value::as_str) == Some(expected_type) => {
            if let Some(obj) = b.as_object_mut() {
                obj.insert(f.field.to_string(), body);
                obj.remove(f.truncated_flag);
                obj.remove(f.bytes_flag);
            }
        }
        _ => {
            tracing::warn!(
                block_ordinal,
                kind,
                "payload row does not match its content block; serving stored preview"
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
        let (slim, rows) = extract_payloads(content.clone()).unwrap();
        assert!(rows.is_empty());
        assert_eq!(slim, content);
    }

    #[test]
    fn oversized_bodies_extract_and_splice_back_identically() {
        let content = json!([
            {"type": "tool_use", "id": "t1", "name": "bash", "input": {"cmd": big_string()}},
            {"type": "text", "text": "between"},
            {"type": "tool_result", "toolCallId": "t1", "output": big_string()},
        ]);
        assert!(needs_extraction(&content));
        let (mut slim, rows) = extract_payloads(content.clone()).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].block_ordinal, 0);
        assert_eq!(rows[0].kind, KIND_TOOL_USE_INPUT);
        assert_eq!(rows[1].block_ordinal, 2);
        assert_eq!(rows[1].kind, KIND_TOOL_RESULT_OUTPUT);
        // The stored placeholder is the shared slim-projection preview plus
        // the serve-time flag names — a slim page read serves it as-is.
        assert_eq!(slim[0]["inputTruncated"], json!(true));
        assert_eq!(
            slim[0]["inputBytes"],
            json!(slim_body_size(&content[0]["input"]))
        );
        let preview_cmd = slim[0]["input"]["cmd"].as_str().unwrap();
        assert!(preview_cmd.len() < big_string().len());
        assert!(big_string().starts_with(preview_cmd));
        assert_eq!(slim[2]["outputTruncated"], json!(true));
        assert_eq!(slim[2]["outputBytes"], json!(big_string().len()));
        assert!(slim[2]["output"].as_str().unwrap().len() < big_string().len());
        // The stored form must NOT re-extract (idempotent write path).
        assert!(!needs_extraction(&slim));
        // Repetitive bodies compress.
        assert_eq!(rows[0].encoding, ENCODING_ZLIB);
        for row in rows {
            let body = decode_body(row.encoding, &row.body).unwrap();
            splice_payload(&mut slim, row.block_ordinal, row.kind, body);
        }
        assert_eq!(slim, content);
        // Flags stripped, body restored: the re-serialized bytes match.
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

    #[test]
    fn block_extraction_matches_whole_message_extraction() {
        let heavy = json!({"type": "tool_use", "id": "t1", "name": "bash",
            "input": {"cmd": big_string()}});
        let small = json!({"type": "text", "text": "hi"});
        assert!(block_needs_extraction(&heavy));
        assert!(!block_needs_extraction(&small));

        let (slim_small, row) = extract_block_payload(small.clone(), 3).unwrap();
        assert!(row.is_none());
        assert_eq!(slim_small, small);

        // The single-block path at ordinal 5 produces the same placeholder
        // and body as the whole-message path (which sees the block at 0).
        let (slim_block, row) = extract_block_payload(heavy.clone(), 5).unwrap();
        let row = row.unwrap();
        assert_eq!(row.block_ordinal, 5);
        assert_eq!(row.kind, KIND_TOOL_USE_INPUT);
        let (whole_slim, whole_rows) = extract_payloads(json!([heavy.clone()])).unwrap();
        assert_eq!(slim_block, whole_slim[0]);
        assert_eq!(row.encoding, whole_rows[0].encoding);
        assert_eq!(row.body, whole_rows[0].body);

        // A block already slimmed to its placeholder form re-extracts as a
        // no-op (idempotent), and hydration restores the original exactly.
        let (again, no_row) = extract_block_payload(slim_block.clone(), 5).unwrap();
        assert!(no_row.is_none());
        assert_eq!(again, slim_block);
        let mut content = json!([slim_block]);
        let body = decode_body(row.encoding, &row.body).unwrap();
        splice_payload(&mut content, 0, row.kind, body);
        assert_eq!(content[0], heavy);
    }

    #[test]
    fn placeholder_keys_lists_flagged_blocks_only() {
        let content = json!([
            {"type": "tool_use", "id": "t1", "name": "bash",
             "input": {"cmd": "preview"}, "inputTruncated": true, "inputBytes": 99999},
            {"type": "text", "text": "between"},
            {"type": "tool_result", "toolCallId": "t1", "output": "inline"},
            {"type": "tool_result", "toolCallId": "t2", "output": "preview",
             "outputTruncated": true, "outputBytes": 88888},
        ]);
        assert_eq!(
            placeholder_keys(&content),
            vec![(0, KIND_TOOL_USE_INPUT), (3, KIND_TOOL_RESULT_OUTPUT)]
        );
        assert!(placeholder_keys(&json!("bare string")).is_empty());
    }
}
