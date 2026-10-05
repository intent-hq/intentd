//! Receipt-owned bounded keyset reads. Current service authorization is required
//! before and after this call; no live-note hydration or revision comparison.
use crate::Store;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use intent_core::{
    note_mutation::NoteMutationError,
    note_page::NotePageError,
    note_receipt_detail::{ReceiptDetailKind, ReceiptDetailQuery},
    Error, Result,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;
mod context;
mod detail;

const RECORD_PAGE_SQL: &str = "SELECT sequence,value FROM note_operation_item WHERE operation_key=? AND kind=? AND sequence>=? ORDER BY sequence LIMIT ?";

fn db(error: impl std::fmt::Display) -> Error {
    Error::Internal(format!("receipt detail storage: {error}"))
}
fn invalid() -> Error {
    Error::NotePage(NotePageError::CursorInvalid)
}
fn expired() -> Error {
    Error::NotePage(NotePageError::Expired)
}
fn budget() -> Error {
    Error::NotePage(NotePageError::Budget)
}
fn frame_len(value: &Value, id: &Value) -> usize {
    json!({"jsonrpc":"2.0","id":id,"result":value})
        .to_string()
        .len()
}
fn safe(n: &Value) -> bool {
    n.as_u64().is_some_and(|n| n <= 9_007_199_254_740_991)
}
fn token(s: &Value) -> bool {
    s.as_str()
        .is_some_and(|s| !s.is_empty() && s.len() <= 256 && !s.contains('\0'))
}

fn fields(value: &Value, count: usize) -> bool {
    value.as_object().is_some_and(|o| o.len() == count)
}
fn digest(value: &Value) -> bool {
    value.as_str().is_some_and(|s| {
        s.len() == 64
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}
fn effect(item: &Value) -> bool {
    match item["kind"].as_str() {
        Some("createdTask") => fields(item, 2) && token(&item["taskNoteId"]),
        Some("warning") => {
            (fields(item, 4) || (fields(item, 5) && token(&item["detailRef"])))
                && token(&item["code"])
                && item["messagePreview"]
                    .as_str()
                    .is_some_and(|s| s.len() <= 512)
                && item["truncated"].is_boolean()
        }
        Some("annotationInvalidation") => {
            fields(item, 4)
                && token(&item["sourceRevision"])
                && token(&item["attributionGeneration"])
                && token(&item["commentRevision"])
        }
        Some("sourceEffect") => {
            fields(item, 9)
                && matches!(
                    item["reason"].as_str(),
                    Some(
                        "task-conversion"
                            | "anchor-repair"
                            | "phantom-scrub"
                            | "task-marker-projection"
                    )
                )
                && token(&item["inputState"])
                && token(&item["outputState"])
                && fields(&item["range"], 2)
                && safe(&item["range"]["start"])
                && safe(&item["range"]["end"])
                && item["range"]["start"].as_u64() <= item["range"]["end"].as_u64()
                && safe(&item["insertedLength"])
                && digest(&item["beforeDigest"])
                && digest(&item["afterDigest"])
                && token(&item["detailRef"])
        }
        _ => false,
    }
}

fn cursor(key: &[u8], binding: &[u8; 32], next: u64) -> Result<String> {
    let mut bytes = Vec::from(next.to_be_bytes());
    bytes.extend_from_slice(binding);
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(db)?;
    mac.update(&bytes);
    bytes.extend_from_slice(&mac.finalize().into_bytes());
    Ok(format!("nr1.{}", URL_SAFE_NO_PAD.encode(bytes)))
}
fn position(text: &str, key: &[u8], binding: &[u8; 32]) -> Result<i64> {
    let bytes = URL_SAFE_NO_PAD
        .decode(text.strip_prefix("nr1.").ok_or_else(invalid)?)
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
    i64::try_from(u64::from_be_bytes(
        bytes[..8].try_into().map_err(|_| invalid())?,
    ))
    .map_err(|_| invalid())
}

fn validate_item(kind: ReceiptDetailKind, item: &Value) -> Result<()> {
    let valid = match kind {
        ReceiptDetailKind::Detail => detail::valid_record(item),
        ReceiptDetailKind::InverseText => false,
        ReceiptDetailKind::Mapping => {
            item.as_object().is_some_and(|o| o.len() == 3)
                && safe(&item["start"])
                && safe(&item["end"])
                && safe(&item["insertedLength"])
                && item["start"].as_u64() <= item["end"].as_u64()
        }
        ReceiptDetailKind::Effects => effect(item),
        ReceiptDetailKind::Inverse => {
            item.as_object().is_some_and(|o| o.len() == 8)
                && token(&item["historyGroup"])
                && safe(&item["ordinal"])
                && safe(&item["start"])
                && safe(&item["end"])
                && item["start"].as_u64() <= item["end"].as_u64()
                && token(&item["inputState"])
                && token(&item["outputState"])
                && token(&item["provenanceRef"])
                && fields(&item["replacement"], 4)
                && token(&item["replacement"]["textId"])
                && safe(&item["replacement"]["length"])
                && safe(&item["replacement"]["utf8Bytes"])
                && digest(&item["replacement"]["sha256"])
        }
    };
    if valid {
        Ok(())
    } else {
        Err(Error::Internal("invalid retained receipt record".into()))
    }
}

impl Store {
    /// Read immutable committed mapping/effects/inverse records by indexed keyset.
    /// The service owns current authorization, including original deleted scope.
    /// # Errors
    /// Returns typed cursor/expiry/budget/digest errors without note/source data.
    #[expect(clippy::too_many_lines)]
    pub async fn read_note_receipt_detail(
        &self,
        principal: &str,
        query: &ReceiptDetailQuery,
        rpc_id: &Value,
    ) -> Result<Value> {
        query.validate().map_err(Error::NoteMutation)?;
        if principal.is_empty()
            || principal.len() > 256
            || principal.contains('\0')
            || !(rpc_id.as_str().is_some_and(|s| s.len() <= 64)
                || rpc_id
                    .as_i64()
                    .is_some_and(|n| (-9_007_199_254_740_991..=9_007_199_254_740_991).contains(&n)))
        {
            return Err(Error::NoteMutation(NoteMutationError::Invalid));
        }
        let mut tx = self.read_pool().begin().await.map_err(db)?;
        let backend =
            sqlx::query("SELECT backend_id,token_key FROM note_page_backend WHERE singleton=1")
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        if backend.get::<String, _>("backend_id") != query.scope.backend_id {
            return Err(invalid());
        }
        let workspace: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM workspace WHERE id=?)")
                .bind(&query.scope.workspace_id)
                .fetch_one(&mut *tx)
                .await
                .map_err(db)?;
        if !workspace {
            return Err(Error::NotFound("Workspace not found".into()));
        }
        let row=sqlx::query("SELECT operation_key,payload_digest,retain_until,outcome,converted_count FROM note_operation WHERE principal=? AND backend_id=? AND workspace_id=? AND note_id=? AND instance_id=? AND operation_id=?")
            .bind(principal).bind(&query.scope.backend_id).bind(&query.scope.workspace_id).bind(&query.scope.note_id)
            .bind(&query.scope.note_instance_id).bind(&query.operation_id).fetch_optional(&mut *tx).await.map_err(db)?.ok_or_else(invalid)?;
        let retain_until: i64 = row.get("retain_until");
        let now = || i64::try_from(intent_core::now_epoch_ms() / 1000).map_err(db);
        if retain_until <= now()? {
            return Err(expired());
        }
        let digest: String = row.get("payload_digest");
        if query.payload_digest.as_ref().is_some_and(|d| d != &digest) {
            return Err(Error::NoteMutation(NoteMutationError::Mismatch));
        }
        let raw: String = row.get("outcome");
        let receipt: Value = serde_json::from_str(&raw).map_err(db)?;
        if receipt["kind"] != "noteCommitReceipt"
            || receipt["outcome"] != "committed"
            || receipt["scope"] != serde_json::to_value(&query.scope).map_err(db)?
            || !token(&receipt["beforeRevision"])
            || !token(&receipt["afterRevision"])
            || !safe(&receipt["sourceLength"])
            || receipt["operationId"] != query.operation_id
            || receipt["payloadDigest"] != digest
        {
            return Err(invalid());
        }
        let receipt_expiry = receipt["receiptExpiresAt"]
            .as_str()
            .and_then(intent_core::parse_iso)
            .ok_or_else(invalid)?;
        if receipt_expiry.unix_timestamp() != retain_until {
            return Err(invalid());
        }
        let operation_key: String = row.get("operation_key");
        let owns_reference = if query.kind == ReceiptDetailKind::Detail {
            sqlx::query_scalar::<_,bool>("SELECT EXISTS(SELECT 1 FROM note_operation_detail WHERE operation_key=? AND reference=?)")
                .bind(&operation_key).bind(&query.reference).fetch_one(&mut *tx).await.map_err(db)?
        } else {
            receipt[query.kind.reference_field()] == query.reference
        };
        if !owns_reference {
            return Err(invalid());
        }
        let key: Vec<u8> = backend.get("token_key");
        let binding: [u8; 32] = Sha256::digest(
            serde_json::to_vec(&json!([
                principal,
                query,
                operation_key,
                retain_until,
                receipt
            ]))
            .map_err(db)?,
        )
        .into();
        let after = query
            .cursor
            .as_ref()
            .map(|c| position(c, &key, &binding))
            .transpose()?
            .unwrap_or(i64::try_from(query.offset.unwrap_or(0)).map_err(|_| invalid())?);
        let mut out = json!({"scope":query.scope,"operationId":query.operation_id,"beforeRevision":receipt["beforeRevision"],"afterRevision":receipt["afterRevision"],"items":[],"nextCursor":null});
        if query.operation_envelope {
            let length = if matches!(
                query.kind,
                ReceiptDetailKind::Inverse
                    | ReceiptDetailKind::InverseText
                    | ReceiptDetailKind::Detail
            ) {
                receipt["sourceLength"].clone()
            } else {
                let length:Option<i64>=sqlx::query_scalar("SELECT end FROM note_operation_source WHERE operation_key=? AND phase='base' ORDER BY start DESC LIMIT 1")
                    .bind(&operation_key).fetch_optional(&mut *tx).await.map_err(db)?;
                json!(length.unwrap_or(0))
            };
            out["kind"] = json!("noteOperationPage");
            out["outputKind"] = json!(query.kind);
            out["payloadDigest"] = json!(digest);
            out["sourceLength"] = length;
            out["expiresAt"] = receipt["receiptExpiresAt"].clone();
        } else if query.context_envelope {
            out = json!({"kind":"noteContextPage","scope":query.scope,"sourceRevision":receipt["afterRevision"],"snapshotId":operation_key,"expiresAt":receipt["receiptExpiresAt"],"items":[],"nextCursor":null});
        } else {
            out["kind"] = json!(if query.kind == ReceiptDetailKind::Mapping {
                "noteMappingPage"
            } else {
                "noteEffectsPage"
            });
        }
        if query.kind == ReceiptDetailKind::Effects {
            let converted = u64::try_from(row.get::<i64, _>("converted_count")).map_err(db)?;
            if converted > 9_007_199_254_740_991 {
                return Err(invalid());
            }
            out["convertedCount"] = json!(converted);
        }
        if query.kind == ReceiptDetailKind::InverseText {
            detail::inverse_text(
                &mut tx,
                &operation_key,
                query,
                after,
                &mut out,
                &key,
                &binding,
                rpc_id,
            )
            .await?;
        } else {
            if query.offset.is_some() {
                return Err(invalid());
            }
            let rows=sqlx::query(if query.kind==ReceiptDetailKind::Detail {
                "SELECT sequence,value FROM note_operation_detail WHERE operation_key=? AND reference=? AND sequence>=? ORDER BY sequence LIMIT ?"
            } else { RECORD_PAGE_SQL })
                .bind(&operation_key).bind(if query.kind==ReceiptDetailKind::Detail {query.reference.as_str()}else{query.kind.storage_kind()})
                .bind(after).bind(i64::try_from(query.max_items+1).map_err(db)?).fetch_all(&mut *tx).await.map_err(db)?;
            let mut items = Vec::new();
            let mut fragment_bytes = 0;
            for (index, row) in rows.iter().take(query.max_items).enumerate() {
                let value: String = row.get("value");
                let item: Value = serde_json::from_str(&value).map_err(db)?;
                validate_item(query.kind, &item)?;
                let source_bytes = if item["kind"] == "fragment" {
                    item["text"].as_str().ok_or_else(invalid)?.len()
                } else {
                    0
                };
                if fragment_bytes + source_bytes > query.max_source_bytes {
                    if index == 0 {
                        return Err(budget());
                    }
                    break;
                }
                fragment_bytes += source_bytes;

                let seq: i64 = row.get("sequence");
                let next = u64::try_from(seq)
                    .map_err(db)?
                    .checked_add(1)
                    .ok_or_else(invalid)?;
                let previous = out.clone();
                items.push(item);
                out["items"] = json!(items);
                out["nextCursor"] = if index + 1 < rows.len() {
                    json!(cursor(&key, &binding, next)?)
                } else {
                    Value::Null
                };
                if frame_len(&out, rpc_id) > query.max_wire_bytes {
                    if index == 0 {
                        return Err(budget());
                    }
                    out = previous;
                    break;
                }
            }
        }
        if frame_len(&out, rpc_id) > query.max_wire_bytes {
            return Err(budget());
        }
        tx.commit().await.map_err(db)?;
        if retain_until <= now()? {
            return Err(expired());
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests;
