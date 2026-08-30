//! Payload codec for the `agent_message_payload` side table (0107,
//! intent-hq/intent#3884).
//!
//! Externalized heavy block bodies (`tool_result.output`, oversized
//! `tool_use.input`, thumbnails) are stored compressed: [`encode_payload`]
//! zstd-compresses bodies of at least [`PAYLOAD_COMPRESS_MIN_BYTES`] and
//! keeps the compressed form only when it is actually smaller (small or
//! incompressible bodies store `encoding='none'` verbatim);
//! [`decode_payload`] reverses the transform keyed by the row's `encoding`
//! marker. Unknown markers fail loudly — they mean the row was written by a
//! newer intentd with a codec this build does not know.
//!
//! [`externalize_content`] is the write-path entry point: it scans a message
//! content array for heavy `tool_use.input` / `tool_result.output` bodies
//! (strictly over the slim projection budget), replaces each in place with a
//! [`payload_ref_marker`] envelope, and returns the [`PayloadRow`]s to insert
//! alongside the message. The side-table row — keyed by
//! `(message_id, block_ordinal, kind)` — is the source of truth on read; the
//! marker is a self-describing hint carrying the body's byte size and a
//! slim-budget preview so slim reads never touch the side table.

use std::borrow::Cow;

use intent_core::{Error, Result};

/// `encoding` marker for a body stored verbatim.
pub const PAYLOAD_ENCODING_NONE: &str = "none";

/// `encoding` marker for a zstd-compressed body.
pub const PAYLOAD_ENCODING_ZSTD: &str = "zstd";

/// `kind` marker: a `tool_result` block's `output` field value (JSON bytes).
pub const PAYLOAD_KIND_TOOL_RESULT_OUTPUT: &str = "tool_result_output";

/// `kind` marker: an oversized `tool_use` block's `input` field value (JSON bytes).
pub const PAYLOAD_KIND_TOOL_USE_INPUT: &str = "tool_use_input";

/// `kind` marker: the per-message thumbnail JSON map (0097 layout, one row
/// per message at `block_ordinal` 0).
pub const PAYLOAD_KIND_THUMBNAILS: &str = "thumbnails";

/// Bodies below this size skip compression outright: the zstd frame overhead
/// dominates and the write-lock benefit is nil for bodies this small.
pub const PAYLOAD_COMPRESS_MIN_BYTES: usize = 512;

/// Marker key of a reference envelope left in `agent_message.content` where
/// an externalized body used to be. The value is [`PAYLOAD_REF_VERSION`].
/// The marker is a self-describing hint only — the read path rehydrates from
/// the `agent_message_payload` row keyed by `(message_id, block_ordinal,
/// kind)`, so inline content that merely mimics this key can never address
/// another message's payload.
pub const PAYLOAD_REF_KEY: &str = "intentPayloadRef";

/// Version stamped as the [`PAYLOAD_REF_KEY`] value. Bump when the envelope
/// shape changes; readers treat unknown versions as an opaque body.
pub const PAYLOAD_REF_VERSION: i64 = 1;

/// A body is externalized only when its [`intent_core::slim_body_size`] is
/// strictly over this bound (the slim projection budget): anything at or
/// under it is served whole by slim reads anyway, and anything over it is
/// exactly what slim previews — so the marker's embedded preview lets slim
/// reads skip the side table entirely.
pub const PAYLOAD_EXTERNALIZE_MIN_BYTES: usize = intent_core::SLIM_PROJECTION_BUDGET_BYTES;

/// One `agent_message_payload` row to insert alongside its message, produced
/// by [`externalize_content`] (or the thumbnail write path). The body is
/// already encoded — insert `encoding` + `body` verbatim.
#[derive(Debug)]
pub struct PayloadRow {
    /// The block's 0-based index in the content array (`thumbnails` rows use
    /// 0 — one map per message).
    pub block_ordinal: i64,
    /// `kind` column marker (one of the `PAYLOAD_KIND_*` constants).
    pub kind: &'static str,
    /// `encoding` column marker from [`encode_payload`].
    pub encoding: &'static str,
    /// The encoded body bytes.
    pub body: Vec<u8>,
}

/// Encode a raw body into a [`PayloadRow`] via [`encode_payload`].
#[must_use]
pub fn payload_row(block_ordinal: i64, kind: &'static str, raw: &[u8]) -> PayloadRow {
    let (encoding, stored) = encode_payload(raw);
    PayloadRow {
        block_ordinal,
        kind,
        encoding,
        body: stored.into_owned(),
    }
}

/// The reference envelope replacing an externalized field value:
/// `{ "intentPayloadRef": 1, "kind": …, "bytes": …, "preview": … }` where
/// `bytes` is the original body's [`intent_core::slim_body_size`] and
/// `preview` is its [`intent_core::cap_json_value`] slim-budget preview —
/// the same bound the serve-time slim projection applies, so slim reads can
/// serve the marker's preview without touching the side table.
#[must_use]
pub fn payload_ref_marker(
    kind: &str,
    bytes: usize,
    original: &serde_json::Value,
) -> serde_json::Value {
    let mut budget = intent_core::SLIM_PROJECTION_BUDGET_BYTES;
    let mut marker = serde_json::Map::new();
    marker.insert(PAYLOAD_REF_KEY.to_string(), PAYLOAD_REF_VERSION.into());
    marker.insert("kind".to_string(), kind.into());
    marker.insert("bytes".to_string(), bytes.into());
    marker.insert(
        "preview".to_string(),
        intent_core::cap_json_value(original, &mut budget),
    );
    serde_json::Value::Object(marker)
}

/// Write-path externalization (intent-hq/intent#3884): scan a message
/// content array for `tool_use` / `tool_result` blocks whose `input` /
/// `output` body measures strictly over [`PAYLOAD_EXTERNALIZE_MIN_BYTES`],
/// replace each such field value in place with a [`payload_ref_marker`],
/// and return the encoded [`PayloadRow`]s to insert alongside the message
/// (empty for the common light message — non-array content and light blocks
/// pass through untouched). A body that fails to serialize is left inline
/// (WARN) — externalization is an optimization, never worth failing the
/// message write. Callers MUST run this (compression is CPU work) BEFORE
/// opening the write transaction.
#[must_use]
pub fn externalize_content(content: &mut serde_json::Value) -> Vec<PayloadRow> {
    let Some(blocks) = content.as_array_mut() else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    for (ordinal, block) in blocks.iter_mut().enumerate() {
        let Some(obj) = block.as_object_mut() else {
            continue;
        };
        let (field, kind) = match obj.get("type").and_then(serde_json::Value::as_str) {
            Some("tool_use") => ("input", PAYLOAD_KIND_TOOL_USE_INPUT),
            Some("tool_result") => ("output", PAYLOAD_KIND_TOOL_RESULT_OUTPUT),
            _ => continue,
        };
        let Some(body) = obj.get(field) else {
            continue;
        };
        let bytes = intent_core::slim_body_size(body);
        if bytes <= PAYLOAD_EXTERNALIZE_MIN_BYTES {
            continue;
        }
        let raw = match serde_json::to_vec(body) {
            Ok(raw) => raw,
            Err(e) => {
                tracing::warn!(
                    ordinal,
                    kind,
                    error = %e,
                    "serialize heavy block body failed; leaving it inline"
                );
                continue;
            }
        };
        let ordinal_i64 = i64::try_from(ordinal).unwrap_or(i64::MAX);
        let marker = payload_ref_marker(kind, bytes, body);
        rows.push(payload_row(ordinal_i64, kind, &raw));
        obj.insert(field.to_string(), marker);
    }
    rows
}

/// zstd compression level. Level 3 (the zstd default) compresses the highly
/// repetitive JSON/text tool outputs several-fold while staying fast enough
/// for the write path — higher levels trade write-path CPU for marginal
/// gains on this content.
const ZSTD_LEVEL: i32 = 3;

/// Encode a payload body for storage: returns the `encoding` marker and the
/// bytes to store. Bodies of at least [`PAYLOAD_COMPRESS_MIN_BYTES`] are
/// zstd-compressed; the compressed form is kept only when strictly smaller
/// than the input, otherwise (and for small bodies) the input is stored
/// verbatim under [`PAYLOAD_ENCODING_NONE`]. A zstd failure is non-fatal:
/// the body falls back to `none` with a WARN.
#[must_use]
pub fn encode_payload(body: &[u8]) -> (&'static str, Cow<'_, [u8]>) {
    if body.len() < PAYLOAD_COMPRESS_MIN_BYTES {
        return (PAYLOAD_ENCODING_NONE, Cow::Borrowed(body));
    }
    match zstd::bulk::compress(body, ZSTD_LEVEL) {
        Ok(compressed) if compressed.len() < body.len() => {
            (PAYLOAD_ENCODING_ZSTD, Cow::Owned(compressed))
        }
        Ok(_) => (PAYLOAD_ENCODING_NONE, Cow::Borrowed(body)),
        Err(e) => {
            tracing::warn!(
                body_len = body.len(),
                error = %e,
                "zstd compression failed; storing payload uncompressed"
            );
            (PAYLOAD_ENCODING_NONE, Cow::Borrowed(body))
        }
    }
}

/// Decode a stored payload body keyed by its `encoding` marker: `none`
/// returns the bytes as-is, `zstd` decompresses.
///
/// # Errors
///
/// Returns `Error::Internal` on an unknown `encoding` marker (a row written
/// by a newer intentd build) or a corrupt zstd frame.
pub fn decode_payload(encoding: &str, body: Vec<u8>) -> Result<Vec<u8>> {
    match encoding {
        PAYLOAD_ENCODING_NONE => Ok(body),
        PAYLOAD_ENCODING_ZSTD => zstd::stream::decode_all(body.as_slice())
            .map_err(|e| Error::Internal(format!("zstd decompression failed: {e}"))),
        other => Err(Error::Internal(format!(
            "unknown agent_message_payload encoding '{other}' — this row was \
             written by a newer intentd build; upgrade intentd to read it"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;

    /// A unique temp DB path cleaned up on drop (mirrors `crate::tests::TempDb`,
    /// which is private to that module).
    struct TempDb {
        path: std::path::PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("test-payload-{}.db", uuid::Uuid::new_v4()));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let p = std::path::PathBuf::from(format!("{}{suffix}", self.path.display()));
                let _ = std::fs::remove_file(p);
            }
        }
    }

    /// A compressible body at or above the threshold round-trips through
    /// zstd: the stored form is marked `zstd`, is smaller than the input,
    /// and decodes back to the exact input bytes.
    #[test]
    fn compressible_body_round_trips_as_zstd() {
        let body = "{\"output\":\"result line\"}".repeat(200).into_bytes();
        assert!(body.len() >= PAYLOAD_COMPRESS_MIN_BYTES);
        let (encoding, stored) = encode_payload(&body);
        assert_eq!(encoding, PAYLOAD_ENCODING_ZSTD);
        assert!(stored.len() < body.len());
        let decoded = decode_payload(encoding, stored.into_owned()).expect("decode");
        assert_eq!(decoded, body);
    }

    /// A body below the threshold skips compression and round-trips verbatim
    /// under `none`.
    #[test]
    fn small_body_stays_uncompressed() {
        let body = vec![b'x'; PAYLOAD_COMPRESS_MIN_BYTES - 1];
        let (encoding, stored) = encode_payload(&body);
        assert_eq!(encoding, PAYLOAD_ENCODING_NONE);
        assert_eq!(stored.as_ref(), body.as_slice());
        let decoded = decode_payload(encoding, stored.into_owned()).expect("decode");
        assert_eq!(decoded, body);
    }

    /// An incompressible body at threshold size falls back to `none` (the
    /// zstd form would be larger) and round-trips verbatim.
    #[test]
    fn incompressible_body_falls_back_to_none() {
        // Deterministic high-entropy bytes: a simple xorshift stream defeats
        // zstd at this size.
        let mut state: u64 = 0x9e37_79b9_7f4a_7c15;
        let body: Vec<u8> = (0..PAYLOAD_COMPRESS_MIN_BYTES)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state & 0xff) as u8
            })
            .collect();
        let (encoding, stored) = encode_payload(&body);
        assert_eq!(encoding, PAYLOAD_ENCODING_NONE);
        let decoded = decode_payload(encoding, stored.into_owned()).expect("decode");
        assert_eq!(decoded, body);
    }

    /// An unknown encoding marker fails with a clear error naming the marker
    /// instead of serving garbage bytes.
    #[test]
    fn unknown_encoding_fails_loudly() {
        let err = decode_payload("brotli", vec![1, 2, 3]).expect_err("must fail");
        let msg = err.to_string();
        assert!(msg.contains("brotli"), "error names the marker: {msg}");
        assert!(
            msg.contains("newer intentd"),
            "error explains the cause: {msg}"
        );
    }

    /// A corrupt zstd frame surfaces a decode error rather than panicking.
    #[test]
    fn corrupt_zstd_frame_errors() {
        assert!(decode_payload(PAYLOAD_ENCODING_ZSTD, vec![0xde, 0xad, 0xbe, 0xef]).is_err());
    }

    /// Heavy `tool_use.input` / `tool_result.output` bodies are replaced by
    /// versioned reference markers with correct ordinals/kinds, the payload
    /// rows decode back to the exact original bodies, and light blocks /
    /// text blocks pass through untouched.
    #[test]
    fn externalize_replaces_heavy_bodies_and_keeps_light_inline() {
        let big_out = "output line\n".repeat(400);
        let big_input = serde_json::json!({ "path": "/tmp/f", "content": "x".repeat(PAYLOAD_EXTERNALIZE_MIN_BYTES * 2) });
        let mut content = serde_json::json!([
            { "type": "text", "text": "hello" },
            { "type": "tool_use", "id": "m:1", "name": "write_file", "input": big_input, "toolCallId": "t1" },
            { "type": "tool_result", "toolCallId": "t1", "output": big_out },
            { "type": "tool_result", "toolCallId": "t2", "output": "small" },
        ]);
        let rows = externalize_content(&mut content);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].block_ordinal, 1);
        assert_eq!(rows[0].kind, PAYLOAD_KIND_TOOL_USE_INPUT);
        assert_eq!(rows[1].block_ordinal, 2);
        assert_eq!(rows[1].kind, PAYLOAD_KIND_TOOL_RESULT_OUTPUT);

        // Payload rows decode back to the exact original field values.
        let decoded_input =
            decode_payload(rows[0].encoding, rows[0].body.clone()).expect("decode input");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&decoded_input).expect("json"),
            big_input
        );
        let decoded_out =
            decode_payload(rows[1].encoding, rows[1].body.clone()).expect("decode output");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&decoded_out).expect("json"),
            serde_json::Value::String(big_out.clone())
        );

        // Heavy fields became markers; light ones stayed inline.
        let blocks = content.as_array().expect("array");
        assert_eq!(blocks[0]["text"], "hello");
        let input_marker = &blocks[1]["input"];
        assert_eq!(input_marker[PAYLOAD_REF_KEY], PAYLOAD_REF_VERSION);
        assert_eq!(input_marker["kind"], PAYLOAD_KIND_TOOL_USE_INPUT);
        assert!(input_marker["bytes"].as_u64().unwrap() > PAYLOAD_EXTERNALIZE_MIN_BYTES as u64);
        assert_eq!(input_marker["preview"]["path"], "/tmp/f");
        let out_marker = &blocks[2]["output"];
        assert_eq!(out_marker[PAYLOAD_REF_KEY], PAYLOAD_REF_VERSION);
        assert_eq!(out_marker["kind"], PAYLOAD_KIND_TOOL_RESULT_OUTPUT);
        assert_eq!(
            out_marker["bytes"].as_u64().unwrap(),
            big_out.len() as u64,
            "string bodies measured by raw length, matching slim_body_size"
        );
        assert_eq!(blocks[3]["output"], "small");
        // The marker (envelope + slim preview) stays bounded well under the
        // original body.
        let marker_size = serde_json::to_vec(out_marker).expect("size").len();
        assert!(
            marker_size < PAYLOAD_EXTERNALIZE_MIN_BYTES * 2,
            "marker stays bounded: {marker_size}"
        );
    }

    /// Boundary + shape cases: an exactly-at-budget body stays inline
    /// (strictly-over rule, matching the slim projection's `<=` serve-whole
    /// check), non-array content and blockless/absent fields are untouched.
    #[test]
    fn externalize_boundary_and_shapes() {
        let at_budget = "y".repeat(PAYLOAD_EXTERNALIZE_MIN_BYTES);
        let mut content = serde_json::json!([
            { "type": "tool_use", "id": "m:0", "name": "bash", "input": at_budget, "toolCallId": "t" },
            { "type": "tool_use", "id": "m:1", "name": "noop", "toolCallId": "t2" },
            "bare string block",
        ]);
        let before = content.clone();
        assert!(externalize_content(&mut content).is_empty());
        assert_eq!(content, before, "at-budget and inputless blocks untouched");

        let mut bare = serde_json::json!("plain string message");
        assert!(externalize_content(&mut bare).is_empty());
        assert_eq!(bare, serde_json::json!("plain string message"));
    }

    /// Deleting an `agent_message` row cascades to its payload rows (0107
    /// `ON DELETE CASCADE`), and the session-level cascade reaches them
    /// transitively.
    #[tokio::test]
    async fn payload_rows_cascade_with_message_delete() {
        async fn count(store: &Store) -> i64 {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_message_payload")
                .fetch_one(store.write_pool())
                .await
                .expect("count")
        }

        let db = TempDb::new();
        let store = Store::open(&db.path).await.expect("open");
        let pool = store.write_pool();
        sqlx::query(
            "INSERT INTO workspace (id, title, branch, status, created_at, updated_at) \
             VALUES ('ws1', 't', 'b', 'Active', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
        )
        .execute(pool)
        .await
        .expect("workspace");
        sqlx::query(
            "INSERT INTO agent_session (id, workspace_id, name, status, created_at, updated_at) \
             VALUES ('a1', 'ws1', 'agent', 'idle', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
        )
        .execute(pool)
        .await
        .expect("session");
        for (msg, seq) in [("m1", 1), ("m2", 2)] {
            sqlx::query(
                "INSERT INTO agent_message (id, agent_id, seq, role, content, created_at) \
                 VALUES (?1, 'a1', ?2, 'assistant', '[]', '2026-01-01T00:00:00Z')",
            )
            .bind(msg)
            .bind(seq)
            .execute(pool)
            .await
            .expect("message");
            let (encoding, stored) = encode_payload(b"{\"output\":\"x\"}");
            sqlx::query(
                "INSERT INTO agent_message_payload \
                 (message_id, block_ordinal, kind, encoding, body) \
                 VALUES (?1, 0, ?2, ?3, ?4)",
            )
            .bind(msg)
            .bind(PAYLOAD_KIND_TOOL_RESULT_OUTPUT)
            .bind(encoding)
            .bind(stored.as_ref())
            .execute(pool)
            .await
            .expect("payload");
        }

        assert_eq!(count(&store).await, 2);

        sqlx::query("DELETE FROM agent_message WHERE id = 'm1'")
            .execute(pool)
            .await
            .expect("delete message");
        assert_eq!(count(&store).await, 1);

        sqlx::query("DELETE FROM agent_session WHERE id = 'a1'")
            .execute(pool)
            .await
            .expect("delete session");
        assert_eq!(count(&store).await, 0);
    }
}
