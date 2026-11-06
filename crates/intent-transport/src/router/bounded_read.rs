//! Bounded whole-object reads on the main connection (`file.read`,
//! `git.showFile`, `note.readAsset`).
//!
//! These replies ride the client's main connection, so one large object
//! would head-of-line-block its heartbeat and every other call. Each reply
//! is kept under [`OBJECT_READ_MAX_BYTES`] of JSON for its object field:
//!
//! - With `maxBytes` the reply carries `truncated` and `totalBytes`; text is
//!   cut to the longest prefix (on a character boundary) whose JSON string
//!   fits, and an asset that does not fit is sent without its bytes.
//! - Without `maxBytes` the legacy whole-object reply is served, as before
//!   (see [`REFUSE_OVER_CAP_WITHOUT_MAX_BYTES`]; in refuse mode an object over
//!   the cap answers `-32602` `too-large`, never a shortened legacy reply that
//!   a client could write back).
//!
//! The whole object is fetched with the chunk methods (`file.readChunk`,
//! `git.showFileChunk`, `note.readAssetChunk`), which clients send over a
//! separate transfer connection. Agent tools call the services directly
//! and are not bounded here.

use serde_json::{json, Map, Value};

use super::{invalid_params, RpcErr, INVALID_PARAMS};

/// Most bytes the object field of a bounded read may take as JSON (the
/// encoded string, quotes included). The rest of the frame stays well under
/// the 512,000-byte read budget.
pub(crate) const OBJECT_READ_MAX_BYTES: usize = 480_000;

/// Smallest `maxBytes` honoured; a smaller request is raised to this.
pub(crate) const OBJECT_READ_MIN_BYTES: usize = 4_096;

/// Whether a read without `maxBytes` of an object over the cap is refused
/// (`too-large`). `false` (the decided behavior) serves the whole object to
/// such callers, as before protocol 15.4, up to the outbound frame cap, so
/// older apps and the iOS app keep working; callers that send `maxBytes`
/// (the current app) get the bounded reply. The unit tests here cover both
/// modes, and the router test and the payload sweep's bulk check branch on it.
pub const REFUSE_OVER_CAP_WITHOUT_MAX_BYTES: bool = false;

/// Parse the optional `maxBytes` param, clamped into
/// [[`OBJECT_READ_MIN_BYTES`], [`OBJECT_READ_MAX_BYTES`]]. Absent or `null`
/// is `None` (legacy shape); anything but a positive integer is `-32602`.
pub(super) fn max_bytes_param(params: &Map<String, Value>) -> Result<Option<usize>, RpcErr> {
    match params.get("maxBytes") {
        None | Some(Value::Null) => Ok(None),
        Some(v) => match v.as_u64().filter(|n| *n > 0) {
            Some(n) => Ok(Some(
                usize::try_from(n)
                    .unwrap_or(usize::MAX)
                    .clamp(OBJECT_READ_MIN_BYTES, OBJECT_READ_MAX_BYTES),
            )),
            None => Err(invalid_params("maxBytes must be a positive integer")),
        },
    }
}

/// Bytes `c` takes inside a JSON string as `serde_json` writes it.
fn escaped_len(c: char) -> usize {
    match c {
        '"' | '\\' | '\n' | '\r' | '\t' | '\u{8}' | '\u{c}' => 2,
        c if (c as u32) < 0x20 => 6,
        c => c.len_utf8(),
    }
}

/// Bytes `s` takes as a JSON string, quotes included.
pub(crate) fn json_string_len(s: &str) -> usize {
    2 + s.chars().map(escaped_len).sum::<usize>()
}

/// The longest prefix of `s` ending on a character boundary whose JSON
/// string (quotes included) is at most `max` bytes.
pub(crate) fn json_prefix(s: &str, max: usize) -> &str {
    let mut used = 2;
    for (i, c) in s.char_indices() {
        used += escaped_len(c);
        if used > max {
            return &s[..i];
        }
    }
    s
}

/// `-32602` `too-large`: a legacy (no `maxBytes`) read of an object over the
/// cap.
fn too_large(method: &str, total_bytes: usize, chunk_method: &str) -> RpcErr {
    RpcErr {
        code: INVALID_PARAMS,
        message: format!(
            "{method}: the object is {total_bytes} bytes, over the {OBJECT_READ_MAX_BYTES}-byte reply cap; pass maxBytes for a bounded read and fetch the whole object with {chunk_method}"
        ),
        data: Some(json!({
            "code": "too-large",
            "totalBytes": total_bytes,
            "maxBytes": OBJECT_READ_MAX_BYTES,
            "chunkMethod": chunk_method,
        })),
    }
}

/// Bound a text object. `legacy` builds the pre-contract reply from the
/// whole text (a bare string for `file.read`, `{ content }` for
/// `git.showFile`); with `max` the reply is `{ content, truncated,
/// totalBytes }`.
pub(super) fn bound_text(
    method: &str,
    chunk_method: &str,
    content: String,
    max: Option<usize>,
    legacy: impl FnOnce(String) -> Value,
) -> Result<Value, RpcErr> {
    bound_text_in(
        REFUSE_OVER_CAP_WITHOUT_MAX_BYTES,
        method,
        chunk_method,
        content,
        max,
        legacy,
    )
}

/// [`bound_text`] with the over-cap mode explicit (`refuse_over_cap`, see
/// [`REFUSE_OVER_CAP_WITHOUT_MAX_BYTES`]), so tests cover both modes.
fn bound_text_in(
    refuse_over_cap: bool,
    method: &str,
    chunk_method: &str,
    content: String,
    max: Option<usize>,
    legacy: impl FnOnce(String) -> Value,
) -> Result<Value, RpcErr> {
    let total_bytes = content.len();
    match max {
        None if !refuse_over_cap || json_string_len(&content) <= OBJECT_READ_MAX_BYTES => {
            Ok(legacy(content))
        }
        None => Err(too_large(method, total_bytes, chunk_method)),
        Some(max) => {
            let prefix = json_prefix(&content, max);
            let truncated = prefix.len() < total_bytes;
            Ok(json!({ "content": prefix, "truncated": truncated, "totalBytes": total_bytes }))
        }
    }
}

/// Decoded byte length of standard padded base64 text.
fn base64_decoded_len(data: &str) -> usize {
    let padding = data.bytes().rev().take_while(|b| *b == b'=').count();
    (data.len() / 4 * 3).saturating_sub(padding)
}

/// Bound a `note.readAsset` reply (an object with base64 `data`). With
/// `max` it gains `truncated` and `totalBytes` (decoded asset bytes); an
/// asset whose `data` does not fit is sent with `data: ""`.
pub(super) fn bound_asset(result: Value, max: Option<usize>) -> Result<Value, RpcErr> {
    bound_asset_in(REFUSE_OVER_CAP_WITHOUT_MAX_BYTES, result, max)
}

/// [`bound_asset`] with the over-cap mode explicit (see [`bound_text_in`]).
fn bound_asset_in(
    refuse_over_cap: bool,
    mut result: Value,
    max: Option<usize>,
) -> Result<Value, RpcErr> {
    let data_len = result["data"].as_str().map_or(0, str::len);
    let total_bytes = result["data"].as_str().map_or(0, base64_decoded_len);
    match max {
        None if !refuse_over_cap || data_len + 2 <= OBJECT_READ_MAX_BYTES => Ok(result),
        None => Err(too_large(
            "note.readAsset",
            total_bytes,
            "note.readAssetChunk",
        )),
        Some(max) => {
            let truncated = data_len + 2 > max;
            if truncated {
                result["data"] = Value::String(String::new());
            }
            result["truncated"] = Value::Bool(truncated);
            result["totalBytes"] = json!(total_bytes);
            Ok(result)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[track_caller]
    fn ok(r: Result<Value, RpcErr>) -> Value {
        match r {
            Ok(v) => v,
            Err(e) => panic!("unexpected error: {}", e.message),
        }
    }

    fn params(v: Value) -> Map<String, Value> {
        match v {
            Value::Object(map) => map,
            _ => Map::new(),
        }
    }

    #[test]
    fn max_bytes_is_optional_positive_and_clamped() {
        assert_eq!(max_bytes_param(&params(json!({}))).ok(), Some(None));
        assert_eq!(
            max_bytes_param(&params(json!({ "maxBytes": null }))).ok(),
            Some(None)
        );
        assert_eq!(
            max_bytes_param(&params(json!({ "maxBytes": 1 }))).ok(),
            Some(Some(OBJECT_READ_MIN_BYTES))
        );
        assert_eq!(
            max_bytes_param(&params(json!({ "maxBytes": 100_000 }))).ok(),
            Some(Some(100_000))
        );
        assert_eq!(
            max_bytes_param(&params(json!({ "maxBytes": 10_000_000 }))).ok(),
            Some(Some(OBJECT_READ_MAX_BYTES))
        );
        for bad in [json!(0), json!(-1), json!(1.5), json!("10"), json!(true)] {
            let err = max_bytes_param(&params(json!({ "maxBytes": bad }))).expect_err("bad");
            assert_eq!(err.code, INVALID_PARAMS);
            assert_eq!(err.message, "maxBytes must be a positive integer");
        }
    }

    #[test]
    fn json_lengths_match_serde_json() {
        for s in [
            "",
            "plain",
            "quote \" and \\ slash /",
            "tab\tnl\ncr\r\u{8}\u{c}",
            "ctl \u{1}\u{1f} del \u{7f}",
            "multi é 漢 😀",
        ] {
            let encoded = serde_json::to_string(s).expect("encode");
            assert_eq!(json_string_len(s), encoded.len(), "{s:?}");
        }
    }

    #[test]
    fn prefix_fits_and_never_splits_a_character() {
        let s = "aé漢😀\u{1}\"z".repeat(1_000);
        for max in [2, 3, 4, 5, 7, 10, 100, 4_096] {
            let p = json_prefix(&s, max);
            assert!(json_string_len(p) <= max, "max {max}");
            // One more character would not fit.
            if let Some(next) = s[p.len()..].chars().next() {
                assert!(json_string_len(p) + escaped_len(next) > max, "max {max}");
            }
        }
        assert_eq!(json_prefix("abc", 100), "abc");
    }

    #[test]
    fn text_without_max_bytes_fits_in_either_mode() {
        for refuse in [true, false] {
            let small = ok(bound_text_in(
                refuse,
                "file.read",
                "file.readChunk",
                "hi".into(),
                None,
                Value::String,
            ));
            assert_eq!(small, json!("hi"));
        }
    }

    #[test]
    fn text_over_cap_without_max_bytes_is_refused_in_refuse_mode() {
        let big = "x".repeat(OBJECT_READ_MAX_BYTES);
        let err = bound_text_in(
            true,
            "file.read",
            "file.readChunk",
            big.clone(),
            None,
            Value::String,
        )
        .expect_err("over cap");
        assert_eq!(err.code, INVALID_PARAMS);
        let data = err.data.expect("data");
        assert_eq!(data["code"], "too-large");
        assert_eq!(data["totalBytes"], json!(big.len()));
        assert_eq!(data["maxBytes"], json!(OBJECT_READ_MAX_BYTES));
        assert_eq!(data["chunkMethod"], "file.readChunk");
        // Escaping counts: under the cap in bytes, over it as JSON.
        let quotes = "\"".repeat(OBJECT_READ_MAX_BYTES / 2);
        assert!(bound_text_in(
            true,
            "git.showFile",
            "git.showFileChunk",
            quotes,
            None,
            |c| json!({ "content": c })
        )
        .is_err());
    }

    #[test]
    fn text_over_cap_without_max_bytes_is_whole_in_whole_mode() {
        let big = "x".repeat(OBJECT_READ_MAX_BYTES + 10);
        let whole = ok(bound_text_in(
            false,
            "file.read",
            "file.readChunk",
            big.clone(),
            None,
            Value::String,
        ));
        assert_eq!(whole, Value::String(big.clone()));
        let shown = ok(bound_text_in(
            false,
            "git.showFile",
            "git.showFileChunk",
            big.clone(),
            None,
            |c| json!({ "content": c }),
        ));
        assert_eq!(shown, json!({ "content": big }));
    }

    #[test]
    fn public_entry_points_follow_the_switch() {
        let big = "x".repeat(OBJECT_READ_MAX_BYTES + 10);
        let text = bound_text(
            "file.read",
            "file.readChunk",
            big.clone(),
            None,
            Value::String,
        );
        let asset = bound_asset(
            json!({ "assetId": "a.png", "mimeType": "image/png", "data": big, "sizeKb": 1 }),
            None,
        );
        assert_eq!(text.is_err(), REFUSE_OVER_CAP_WITHOUT_MAX_BYTES);
        assert_eq!(asset.is_err(), REFUSE_OVER_CAP_WITHOUT_MAX_BYTES);
    }

    #[test]
    fn text_with_max_bytes_reports_truncation() {
        let whole = ok(bound_text(
            "file.read",
            "file.readChunk",
            "hello".into(),
            Some(4_096),
            Value::String,
        ));
        assert_eq!(
            whole,
            json!({ "content": "hello", "truncated": false, "totalBytes": 5 })
        );
        let big = "é".repeat(OBJECT_READ_MAX_BYTES);
        let cut = ok(bound_text(
            "file.read",
            "file.readChunk",
            big.clone(),
            Some(OBJECT_READ_MAX_BYTES),
            Value::String,
        ));
        assert_eq!(cut["truncated"], true);
        assert_eq!(cut["totalBytes"], json!(big.len()));
        let content = cut["content"].as_str().expect("content");
        assert!(big.starts_with(content));
        assert!(serde_json::to_string(content).expect("encode").len() <= OBJECT_READ_MAX_BYTES);
        assert!(content.len() > OBJECT_READ_MAX_BYTES - 8);
    }

    #[test]
    fn asset_is_whole_or_sent_without_bytes() {
        let asset = |data: &str| json!({ "assetId": "a.png", "mimeType": "image/png", "data": data, "sizeKb": 1 });
        assert_eq!(ok(bound_asset(asset("aGk="), None)), asset("aGk="));
        let with = ok(bound_asset(asset("aGk="), Some(4_096)));
        assert_eq!(with["data"], "aGk=");
        assert_eq!(with["truncated"], false);
        assert_eq!(with["totalBytes"], 2);
        let big = "QUJD".repeat(OBJECT_READ_MAX_BYTES / 4 + 1);
        let err = bound_asset_in(true, asset(&big), None).expect_err("over cap");
        assert_eq!(err.data.expect("data")["code"], "too-large");
        assert_eq!(ok(bound_asset_in(false, asset(&big), None)), asset(&big));
        let cut = ok(bound_asset(asset(&big), Some(OBJECT_READ_MAX_BYTES)));
        assert_eq!(cut["data"], "");
        assert_eq!(cut["truncated"], true);
        assert_eq!(cut["totalBytes"], json!(big.len() / 4 * 3));
        assert_eq!(cut["assetId"], "a.png");
    }
}
