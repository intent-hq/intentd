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
