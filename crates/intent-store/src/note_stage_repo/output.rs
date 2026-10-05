//! Bounded immutable staged source and supported native selection output.
//! Selection resources must pass the independent frozen-view adapter.
use super::{db, fail, require_workspace, view_read};
use crate::Store;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use intent_core::{
    note_mutation::NoteMutationError,
    note_page::NotePageError,
    note_stage::NoteStageOutput,
    note_stage_read::{NoteStageRead, NoteStageReadKind},
    Error, Result,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;
fn invalid() -> Error {
    Error::NotePage(NotePageError::CursorInvalid)
}
fn deadline(value: &Value) -> Result<String> {
    let text = value["expiresAt"].as_str().ok_or_else(invalid)?;
    let expiry = intent_core::parse_iso(text).ok_or_else(invalid)?;
    if expiry <= intent_core::parse_iso(&intent_core::now_iso()).ok_or_else(invalid)? {
        return Err(Error::NotePage(NotePageError::Expired));
    }
    Ok(text.to_owned())
}
fn cursor(key: &[u8], binding: &[u8; 32], offset: u64) -> Result<String> {
    let mut bytes = offset.to_be_bytes().to_vec();
    bytes.extend_from_slice(binding);
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(db)?;
    mac.update(&bytes);
    bytes.extend_from_slice(&mac.finalize().into_bytes());
    Ok(format!("ns1.{}", URL_SAFE_NO_PAD.encode(bytes)))
}
fn position(text: &str, key: &[u8], binding: &[u8; 32]) -> Result<u64> {
    let bytes = URL_SAFE_NO_PAD
        .decode(text.strip_prefix("ns1.").ok_or_else(invalid)?)
        .map_err(|_| invalid())?;
    if bytes.len() != 72 {
        return Err(invalid());
    }
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(db)?;
    mac.update(&bytes[..40]);
    mac.verify_slice(&bytes[40..]).map_err(|_| invalid())?;
    if bytes[8..40] != binding[..] {
        return Err(invalid());
    }
    Ok(u64::from_be_bytes(
        bytes[..8].try_into().map_err(|_| invalid())?,
    ))
}
fn smaller_prefix(text: &str, bytes: usize) -> usize {
    let first = text.chars().next().map_or(0, char::len_utf8);
    if bytes <= first {
        return 0;
    }
    let mut next = (bytes / 2).max(first);
    while !text.is_char_boundary(next) {
        next -= 1;
    }
    next
}

// Output positions are independent of the complete frozen source extent.
// This scan is confined to the adapter's bounded selection result.
fn selection_piece(text: &str, at: u64, max_bytes: usize) -> Result<(u64, String, u64)> {
    let length = u64::try_from(text.encode_utf16().count()).map_err(db)?;
    if text.is_empty() || at > length {
        return Err(invalid());
    }
    let mut units = 0_u64;
    let mut start = None;
    for (byte, scalar) in text.char_indices() {
        if units == at {
            start = Some(byte);
            break;
        }
        units += u64::try_from(scalar.len_utf16()).expect("scalar width fits");
    }
    let start = start
        .or_else(|| (at == length).then_some(text.len()))
        .ok_or_else(invalid)?;
    let mut end = text.len().min(start.saturating_add(max_bytes));
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    if end == start && at < length {
        return Err(fail(NoteMutationError::Budget));
    }
    let part = &text[start..end];
    Ok((
        at + u64::try_from(part.encode_utf16().count()).map_err(db)?,
        part.to_owned(),
        length,
    ))
}

impl Store {
    /// Read one bounded source or supported selection page from the sealed view.
    /// Current membership is checked by Services before and after this method.
    /// # Errors
    /// Rejects unknown/foreign operation, cancellation/expiry, cursor rebinding,
    /// missing retained pieces, unsupported output adapters and response overflow.
    pub async fn read_note_stage_source(
        &self,
        principal: &str,
        request: &NoteStageRead,
        rpc_id: &Value,
    ) -> Result<Value> {
        request.validate().map_err(fail)?;
        if principal.is_empty()
            || principal.len() > 256
            || principal.contains('\0')
            || !(rpc_id.as_str().is_some_and(|s| s.len() <= 64)
                || rpc_id
                    .as_i64()
                    .is_some_and(|n| (-9_007_199_254_740_991..=9_007_199_254_740_991).contains(&n)))
        {
            return Err(fail(NoteMutationError::Invalid));
        }
        if request.kind == NoteStageReadKind::Search {
            return super::search_output::read(self, principal, request, rpc_id).await;
        }
        let (expected_output, output_kind) = match request.kind {
            NoteStageReadKind::Source => (NoteStageOutput::Source, "source"),
            NoteStageReadKind::SelectionMarkdown => {
                (NoteStageOutput::SelectionMarkdown, "selectionMarkdown")
            }
            NoteStageReadKind::Search => unreachable!("search dispatched above"),
        };
        let mut tx = self.read_pool().begin().await.map_err(db)?;
        require_workspace(&mut tx, &request.workspace_id).await?;
        let row=sqlx::query("SELECT o.operation_key,o.outcome,s.header_digest,s.payload_digest,s.view_id,s.view_length,s.phase,s.header FROM note_operation o JOIN note_stage s USING(operation_key) WHERE o.principal=? AND o.backend_id=? AND o.workspace_id=? AND o.note_id=? AND o.instance_id=? AND o.operation_id=? AND o.method_kind='staged'")
            .bind(principal).bind(&request.backend_id).bind(&request.workspace_id).bind(&request.note_id).bind(&request.note_instance_id).bind(&request.operation_id).fetch_optional(&mut *tx).await.map_err(db)?.ok_or_else(invalid)?;
        if row.get::<String, _>("header_digest") != request.header_digest {
            return Err(invalid());
        }
        if row.get::<String, _>("phase") != "sealed" {
            return Err(Error::NotePage(NotePageError::Expired));
        }
        let header: intent_core::note_stage::NoteStageHeader =
            serde_json::from_str(row.get("header")).map_err(db)?;
        if header.output != expected_output {
            return Err(invalid());
        }
        let state: Value = serde_json::from_str(row.get("outcome")).map_err(db)?;
        let expires = deadline(&state)?;
        let operation: String = row.get("operation_key");
        let view: String = row.get("view_id");
        let payload: String = row.get("payload_digest");
        let length = u64::try_from(row.get::<i64, _>("view_length")).map_err(db)?;
        let generation:i64=sqlx::query_scalar("SELECT generation FROM note_stage_view WHERE operation_key=? ORDER BY generation DESC LIMIT 1").bind(&operation).fetch_one(&mut *tx).await.map_err(db)?;
        let backend =
            sqlx::query("SELECT backend_id,token_key FROM note_page_backend WHERE singleton=1")
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        if backend.get::<String, _>("backend_id") != request.backend_id {
            return Err(invalid());
        }
        let key: Vec<u8> = backend.get("token_key");
        let binding: [u8; 32] = Sha256::digest(
            serde_json::to_vec(&json!([
                principal, request, operation, view, payload, expires, length, generation
            ]))
            .map_err(db)?,
        )
        .into();
        let at = request
            .cursor
            .as_ref()
            .map(|s| position(s, &key, &binding))
            .transpose()?
            .unwrap_or(0);
        let generation = u64::try_from(generation).map_err(db)?;
        let (end, text, output_length) = if request.kind == NoteStageReadKind::Source {
            let (end, text) = view_read::read_piece(
                &mut tx,
                &operation,
                generation,
                length,
                at,
                request.max_source_bytes.unwrap_or(8192),
            )
            .await?;
            (end, text, length)
        } else {
            let output =
                super::selection_output::prepare(&mut tx, &operation, &header, generation, length)
                    .await?;
            selection_piece(&output, at, request.max_source_bytes.unwrap_or(8192))?
        };
        let mut out = json!({"kind":"noteOperationPage","scope":request.scope(),"operationId":request.operation_id,"headerDigest":request.header_digest,"payloadDigest":payload,"viewId":view,"outputKind":output_kind,"sourceLength":length,"items":[],"nextCursor":null,"expiresAt":expires});
        let mut bytes = text.len();
        loop {
            let part = &text[..bytes];
            let next = at + u64::try_from(part.encode_utf16().count()).map_err(db)?;
            if next > end {
                return Err(invalid());
            }
            out["items"] = if part.is_empty() {
                json!([])
            } else {
                json!([{"offset":at,"text":part}])
            };
            out["nextCursor"] = if next == output_length {
                Value::Null
            } else {
                json!(cursor(&key, &binding, next)?)
            };
            if json!({"jsonrpc":"2.0","id":rpc_id,"result":out})
                .to_string()
                .len()
                <= request.max_wire_bytes.unwrap_or(4096)
                && (next > at || at == output_length)
            {
                break;
            }
            if bytes == 0 {
                return Err(fail(NoteMutationError::Budget));
            }
            // Bound repeated serialization by halving the admitted piece. No
            // source is reread or expanded, and every retained prefix is scalar-safe.
            bytes = smaller_prefix(&text, bytes);
        }
        deadline(&state)?;
        tx.commit().await.map_err(db)?;
        // Recheck durable cancellation after leaving the original read snapshot.
        // A cleanup worker cannot publish a partial/terminal-looking success.
        let mut check = self.read_pool().begin().await.map_err(db)?;
        require_workspace(&mut check, &request.workspace_id).await?;
        let current:Option<(String,String)>=sqlx::query_as("SELECT s.phase,o.outcome FROM note_stage s JOIN note_operation o USING(operation_key) WHERE s.operation_key=? AND s.view_id=? AND s.payload_digest=?")
            .bind(&operation).bind(&view).bind(&payload).fetch_optional(&mut *check).await.map_err(db)?;
        let Some((phase, raw)) = current else {
            return Err(Error::NotePage(NotePageError::Expired));
        };
        if phase != "sealed" {
            return Err(Error::NotePage(NotePageError::Expired));
        }
        deadline(&serde_json::from_str(&raw).map_err(db)?)?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn staged_source_frame_reduction_attempts_first_scalar_before_budget_failure() {
        for text in ["😀a", "界a", "éa", "ab"] {
            let first = text.chars().next().unwrap().len_utf8();
            let mut bytes = text.len();
            let mut seen_first = false;
            while bytes > 0 {
                seen_first |= bytes == first;
                let next = super::smaller_prefix(text, bytes);
                assert!(next < bytes && text.is_char_boundary(next));
                bytes = next;
            }
            assert!(seen_first);
        }
    }
}
