//! Capped and paged `agent.getMessageBlock` reads (PROTOCOL §5.5 "Capped and
//! paged block reads", capability `messageBlockPages: 1`).
//!
//! Every served block is bounded by [`MESSAGE_BLOCK_MAX_BYTES`] of compact
//! JSON so the whole response frame stays under the 500 KiB read budget.
//! A block's single large field — its *body* — is cut and paged: a string
//! body pages its own UTF-8 bytes, any other JSON body pages its compact
//! serialization. Image `data` is never paged; an over-cap image is served
//! without `data` and with the slim projection's `dataTruncated` /
//! `dataBytes` flags, so an old client never re-sends a cut-off image.

use serde_json::{json, Value};

use crate::error::{Error, Result};

/// Default and maximum serialized size of the served `result.block`
/// (bytes of compact JSON). Leaves room for the other result fields and the
/// JSON-RPC envelope under the 512,000-byte (500 KiB) read budget.
pub const MESSAGE_BLOCK_MAX_BYTES: usize = 480_000;

/// Page size for agents: the `ws.agent.getMessageBlock` MCP binding
/// defaults `maxBytes` to this value and clamps larger requests down to it,
/// so an agent always pages in 100 KiB pieces (the app's RPC keeps
/// [`MESSAGE_BLOCK_MAX_BYTES`]).
pub const AGENT_MESSAGE_BLOCK_MAX_BYTES: u64 = 102_400;

/// Smallest accepted cap; smaller `maxBytes` values clamp up to it.
pub const MESSAGE_BLOCK_MIN_BYTES: usize = 4_096;

/// The paging request of one `agent.getMessageBlock` call. `None` fields
/// take the documented defaults (offset `0`, cap [`MESSAGE_BLOCK_MAX_BYTES`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MessageBlockPage {
    /// Byte offset into the body text.
    pub offset: Option<u64>,
    /// Requested cap; clamped into
    /// [[`MESSAGE_BLOCK_MIN_BYTES`], [`MESSAGE_BLOCK_MAX_BYTES`]].
    pub max_bytes: Option<u64>,
}

impl MessageBlockPage {
    /// The effective cap after clamping.
    #[must_use]
    pub fn cap(&self) -> usize {
        self.max_bytes.map_or(MESSAGE_BLOCK_MAX_BYTES, |b| {
            usize::try_from(b)
                .unwrap_or(usize::MAX)
                .clamp(MESSAGE_BLOCK_MIN_BYTES, MESSAGE_BLOCK_MAX_BYTES)
        })
    }
}

/// The body field of a block type (PROTOCOL §5.5 body-field table).
fn body_field(block: &Value) -> Option<&'static str> {
    match block.get("type").and_then(Value::as_str) {
        Some("text" | "thinking") => Some("text"),
        Some("tool_use") => Some("input"),
        Some("tool_result") => Some("output"),
        Some("image") => Some("data"),
        _ => None,
    }
}

/// Bytes `value` occupies as compact JSON.
fn serialized_len(value: &Value) -> usize {
    serde_json::to_vec(value).map_or(usize::MAX, |v| v.len())
}

/// Bytes one character costs inside a `serde_json` string literal.
fn escaped_len(c: char) -> usize {
    match c {
        '"' | '\\' | '\u{08}' | '\u{0c}' | '\n' | '\r' | '\t' => 2,
        c if (c as u32) < 0x20 => 6,
        c => c.len_utf8(),
    }
}

/// Assemble the result object.
fn result(
    block: Value,
    truncated: bool,
    field: Option<&str>,
    encoding: Option<&str>,
    total_bytes: Option<usize>,
    next_offset: Option<usize>,
) -> Value {
    let mut out = serde_json::Map::new();
    out.insert("block".to_string(), block);
    out.insert("truncated".to_string(), json!(truncated));
    out.insert("field".to_string(), json!(field));
    out.insert("encoding".to_string(), json!(encoding));
    out.insert("totalBytes".to_string(), json!(total_bytes));
    out.insert("nextOffset".to_string(), json!(next_offset));
    Value::Object(out)
}

/// The `{ id, type }` stub served when nothing else fits. `id`, then
/// `type`, is kept only while the stub still fits `cap`, so an oversized
/// identity field is omitted rather than breaking the cap.
fn stub(block: &Value, cap: usize) -> Value {
    let mut out = serde_json::Map::new();
    for key in ["id", "type"] {
        if let Some(v) = block.get(key) {
            out.insert(key.to_string(), v.clone());
            if serialized_len(&Value::Object(out.clone())) > cap {
                out.remove(key);
            }
        }
    }
    result(Value::Object(out), true, None, None, None, None)
}

/// Apply the cap and paging rules to one full served block and build the
/// `agent.getMessageBlock` result
/// `{ block, truncated, field, encoding, totalBytes, nextOffset }`.
///
/// # Errors
/// `InvalidParams` for an offset past the end of the body text, inside a
/// UTF-8 character, or non-zero on a block without a pageable body.
pub fn page_message_block(mut block: Value, page: &MessageBlockPage) -> Result<Value> {
    let cap = page.cap();
    let offset = usize::try_from(page.offset.unwrap_or(0)).unwrap_or(usize::MAX);
    let field = body_field(&block).filter(|f| block.get(*f).is_some());

    if field == Some("data") {
        let Some(data_len) = block.get("data").and_then(Value::as_str).map(str::len) else {
            return unpaged(block, offset, cap);
        };
        if offset != 0 {
            return Err(offset_unsupported());
        }
        if serialized_len(&block) <= cap {
            return Ok(result(block, false, field, None, Some(data_len), None));
        }
        if let Some(obj) = block.as_object_mut() {
            obj.remove("data");
            obj.insert("dataTruncated".to_string(), json!(true));
            obj.insert("dataBytes".to_string(), json!(data_len));
        }
        if serialized_len(&block) <= cap {
            return Ok(result(block, true, field, None, Some(data_len), None));
        }
        return Ok(stub(&block, cap));
    }

    let Some(field) = field else {
        return unpaged(block, offset, cap);
    };
    let Some(body) = block.get(field) else {
        return unpaged(block, offset, cap);
    };
    let (text, encoding) = match body {
        Value::String(s) => (s.clone(), "text"),
        other => (
            serde_json::to_string(other).map_err(|e| Error::Internal(e.to_string()))?,
            "json",
        ),
    };
    let total = text.len();
    if offset > total {
        return Err(Error::InvalidParams(format!(
            "offset {offset} is past the end of the block body ({total} bytes)"
        )));
    }
    if !text.is_char_boundary(offset) {
        return Err(Error::InvalidParams(format!(
            "offset {offset} is not on a UTF-8 character boundary"
        )));
    }
    if offset == 0 && serialized_len(&block) <= cap {
        return Ok(result(
            block,
            false,
            Some(field),
            Some(encoding),
            Some(total),
            None,
        ));
    }
    block[field] = Value::String(String::new());
    let overhead = serialized_len(&block);
    if overhead > cap {
        return Ok(stub(&block, cap));
    }
    let mut room = cap - overhead;
    let mut end = offset;
    for c in text[offset..].chars() {
        let cost = escaped_len(c);
        if cost > room {
            break;
        }
        room -= cost;
        end += c.len_utf8();
    }
    if end == offset && offset < total {
        return Ok(stub(&block, cap));
    }
    block[field] = Value::String(text[offset..end].to_string());
    let next = (end < total).then_some(end);
    Ok(result(
        block,
        true,
        Some(field),
        Some(encoding),
        Some(total),
        next,
    ))
}

/// A block with no pageable body: served whole when it fits, else stubbed.
fn unpaged(block: Value, offset: usize, cap: usize) -> Result<Value> {
    if offset != 0 {
        return Err(offset_unsupported());
    }
    if serialized_len(&block) <= cap {
        return Ok(result(block, false, None, None, None, None));
    }
    Ok(stub(&block, cap))
}

fn offset_unsupported() -> Error {
    Error::InvalidParams("offset is not supported for this block".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(offset: u64, max: u64) -> MessageBlockPage {
        MessageBlockPage {
            offset: Some(offset),
            max_bytes: Some(max),
        }
    }

    /// Page through a block from offset 0 and return the reassembled body
    /// text, asserting every page's block fits the cap.
    fn reassemble(block: &Value, max: u64) -> (String, usize) {
        let mut out = String::new();
        let mut offset = 0u64;
        let mut pages = 0;
        loop {
            let r = page_message_block(block.clone(), &page(offset, max)).expect("page");
            let cap = page(offset, max).cap();
            assert!(serialized_len(&r["block"]) <= cap, "page fits cap");
            let field = r["field"].as_str().expect("field");
            assert_eq!(r["truncated"], true);
            out.push_str(r["block"][field].as_str().expect("string slice"));
            pages += 1;
            match r["nextOffset"].as_u64() {
                Some(n) => {
                    assert!(n > offset, "progress");
                    offset = n;
                }
                None => return (out, pages),
            }
        }
    }

    #[test]
    fn small_block_is_served_byte_identical() {
        let block = json!({"id": "b1", "type": "tool_use", "name": "x", "input": {"a": 1}});
        let r = page_message_block(block.clone(), &MessageBlockPage::default()).unwrap();
        assert_eq!(r["block"], block);
        assert_eq!(r["truncated"], false);
        assert_eq!(r["field"], "input");
        assert_eq!(r["encoding"], "json");
        assert_eq!(r["totalBytes"], json!(r#"{"a":1}"#.len()));
        assert_eq!(r["nextOffset"], Value::Null);
    }

    #[test]
    fn multi_megabyte_text_pages_reassemble_exactly() {
        let body: String = "héllo \"wörld\"\n\t\u{1}🦀 ".repeat(200_000);
        let block = json!({"id": "b", "type": "text", "text": body});
        let first = page_message_block(block.clone(), &MessageBlockPage::default()).unwrap();
        assert_eq!(first["truncated"], true);
        assert_eq!(first["totalBytes"], json!(body.len()));
        assert!(serialized_len(&first["block"]) <= MESSAGE_BLOCK_MAX_BYTES);
        let (text, pages) = reassemble(&block, MESSAGE_BLOCK_MAX_BYTES as u64);
        assert_eq!(text, body);
        assert!(pages > 1);
        let (small, _) = reassemble(&block, 5_000);
        assert_eq!(small, body);
    }

    #[test]
    fn json_body_pages_parse_back() {
        let output: Vec<Value> = (0..40_000)
            .map(|i| json!({"line": i, "text": format!("row {i} ✓")}))
            .collect();
        let block = json!({"id": "r", "type": "tool_result", "output": output, "is_error": false});
        let (text, _) = reassemble(&block, 100_000);
        let parsed: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(parsed, block["output"]);
        let r = page_message_block(block, &page(0, 100_000)).unwrap();
        assert_eq!(r["encoding"], "json");
        assert_eq!(r["block"]["is_error"], false);
    }

    #[test]
    fn offset_at_end_is_empty_and_final() {
        let block = json!({"id": "b", "type": "text", "text": "abc"});
        let r = page_message_block(block, &page(3, 4096)).unwrap();
        assert_eq!(r["block"]["text"], "");
        assert_eq!(r["truncated"], true);
        assert_eq!(r["nextOffset"], Value::Null);
    }

    #[test]
    fn later_page_of_small_block_is_string_slice() {
        let block = json!({"id": "b", "type": "tool_use", "input": {"k": "v"}});
        let r = page_message_block(block, &page(2, 4096)).unwrap();
        assert_eq!(r["block"]["input"], r#"k":"v"}"#);
        assert_eq!(r["nextOffset"], Value::Null);
    }

    #[test]
    fn invalid_offsets_are_rejected() {
        let block = json!({"id": "b", "type": "text", "text": "aé"});
        let past = page_message_block(block.clone(), &page(9, 4096)).unwrap_err();
        assert!(
            matches!(&past, Error::InvalidParams(m) if m == "offset 9 is past the end of the block body (3 bytes)"),
            "{past:?}"
        );
        let mid = page_message_block(block, &page(2, 4096)).unwrap_err();
        assert!(
            matches!(&mid, Error::InvalidParams(m) if m == "offset 2 is not on a UTF-8 character boundary"),
            "{mid:?}"
        );
        let image = json!({"id": "i", "type": "image", "data": "AAAA", "mimeType": "image/png"});
        let err = page_message_block(image, &page(1, 4096)).unwrap_err();
        assert!(
            matches!(&err, Error::InvalidParams(m) if m == "offset is not supported for this block")
        );
        let other = json!({"id": "o", "type": "resource", "resource": {}});
        assert!(page_message_block(other, &page(1, 4096)).is_err());
    }

    #[test]
    fn over_cap_image_drops_data_with_flags() {
        let data = "A".repeat(2_000_000);
        let block = json!({"id": "i", "type": "image", "data": data, "mimeType": "image/png"});
        let r = page_message_block(block, &MessageBlockPage::default()).unwrap();
        assert_eq!(r["truncated"], true);
        assert_eq!(r["field"], "data");
        assert_eq!(r["encoding"], Value::Null);
        assert_eq!(r["totalBytes"], 2_000_000);
        assert_eq!(r["nextOffset"], Value::Null);
        assert!(r["block"].get("data").is_none());
        assert_eq!(r["block"]["dataTruncated"], true);
        assert_eq!(r["block"]["dataBytes"], 2_000_000);
        assert_eq!(r["block"]["mimeType"], "image/png");
    }

    #[test]
    fn small_image_keeps_data() {
        let block = json!({"id": "i", "type": "image", "data": "AAAA", "mimeType": "image/png"});
        let r = page_message_block(block.clone(), &MessageBlockPage::default()).unwrap();
        assert_eq!(r["block"], block);
        assert_eq!(r["truncated"], false);
        assert_eq!(r["totalBytes"], 4);
    }

    #[test]
    fn oversized_non_body_fields_fall_back_to_stub() {
        let block = json!({"id": "b", "type": "tool_use", "input": "x", "metadata": {"blob": "z".repeat(600_000)}});
        let r = page_message_block(block, &MessageBlockPage::default()).unwrap();
        assert_eq!(r["block"], json!({"id": "b", "type": "tool_use"}));
        assert_eq!(r["truncated"], true);
        assert_eq!(r["field"], Value::Null);
        assert_eq!(r["nextOffset"], Value::Null);
        let unknown =
            json!({"id": "u", "type": "resource", "resource": {"text": "z".repeat(600_000)}});
        let r = page_message_block(unknown, &MessageBlockPage::default()).unwrap();
        assert_eq!(r["block"], json!({"id": "u", "type": "resource"}));
    }

    #[test]
    fn oversized_identity_fields_never_exceed_the_cap() {
        let huge = "x".repeat(600_000);
        let blocks = [
            json!({"id": huge, "type": "text", "text": "a"}),
            json!({"id": "b", "type": huge, "text": "a"}),
            json!({"id": huge, "type": huge}),
            json!({"id": huge, "type": "image", "data": "AAAA"}),
            json!({"id": huge, "type": "tool_result", "output": {"k": 1}}),
        ];
        for max in [None, Some(4_096), Some(100_000)] {
            let p = MessageBlockPage {
                offset: None,
                max_bytes: max,
            };
            for block in &blocks {
                let r = page_message_block(block.clone(), &p).expect("served");
                assert!(serialized_len(&r["block"]) <= p.cap(), "block fits {max:?}");
                assert_eq!(r["truncated"], true);
            }
        }
        let r = page_message_block(blocks[0].clone(), &page(0, 4_096)).unwrap();
        assert_eq!(r["block"], json!({"type": "text"}));
        assert_eq!(r["field"], Value::Null);
        assert_eq!(r["nextOffset"], Value::Null);
        let r = page_message_block(blocks[1].clone(), &page(0, 4_096)).unwrap();
        assert_eq!(r["block"], json!({"id": "b"}));
        let r = page_message_block(blocks[2].clone(), &page(0, 4_096)).unwrap();
        assert_eq!(r["block"], json!({}));
    }

    #[test]
    fn max_bytes_clamps() {
        assert_eq!(page(0, 1).cap(), MESSAGE_BLOCK_MIN_BYTES);
        assert_eq!(page(0, u64::MAX).cap(), MESSAGE_BLOCK_MAX_BYTES);
        assert_eq!(MessageBlockPage::default().cap(), MESSAGE_BLOCK_MAX_BYTES);
        assert_eq!(page(0, 10_000).cap(), 10_000);
    }

    #[test]
    fn escape_heavy_text_still_fits() {
        let body = "\u{1}".repeat(300_000);
        let block = json!({"id": "b", "type": "text", "text": body});
        let (text, _) = reassemble(&block, MESSAGE_BLOCK_MAX_BYTES as u64);
        assert_eq!(text, body);
    }
}
