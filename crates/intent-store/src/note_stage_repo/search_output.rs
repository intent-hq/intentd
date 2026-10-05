//! Source-search pages over a retained immutable view. Matcher replay is bounded
//! separately from newly scanned source; no current Note or full result set is loaded.
use super::{db, fail, require_workspace, search_ranges, view_read};
use crate::Store;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use intent_core::{
    note_mutation::NoteMutationError,
    note_page::NotePageError,
    note_stage::{NoteStageHeader, NoteStageOutput, NoteStageSearchMode},
    note_stage_read::NoteStageRead,
    note_stage_search::{NoteStageSearch, CASE_FOLDING_SHA256, MAX_CARRY_BYTES},
    Error, Result,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{Row, SqliteConnection};

pub(super) fn invalid() -> Error {
    Error::NotePage(NotePageError::CursorInvalid)
}
fn expiry(raw: &Value) -> Result<String> {
    let text = raw["expiresAt"].as_str().ok_or_else(invalid)?;
    let time = intent_core::parse_iso(text).ok_or_else(invalid)?;
    if time <= intent_core::parse_iso(&intent_core::now_iso()).ok_or_else(invalid)? {
        return Err(Error::NotePage(NotePageError::Expired));
    }
    Ok(text.to_owned())
}
pub(super) fn sign(prefix: &str, key: &[u8], binding: &[u8; 32], values: &[u64]) -> Result<String> {
    let mut bytes = Vec::with_capacity(values.len() * 8 + 64);
    for value in values {
        bytes.extend_from_slice(&value.to_be_bytes());
    }
    bytes.extend_from_slice(binding);
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(db)?;
    mac.update(prefix.as_bytes());
    mac.update(&bytes);
    bytes.extend_from_slice(&mac.finalize().into_bytes());
    let token = format!("{prefix}{}", URL_SAFE_NO_PAD.encode(bytes));
    if token.len() > 256 {
        return Err(invalid());
    }
    Ok(token)
}
pub(super) fn verify(
    text: &str,
    prefix: &str,
    key: &[u8],
    binding: &[u8; 32],
    count: usize,
) -> Result<Vec<u64>> {
    if text.len() > 256 {
        return Err(invalid());
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(text.strip_prefix(prefix).ok_or_else(invalid)?)
        .map_err(|_| invalid())?;
    let payload = count * 8 + 32;
    if bytes.len() != payload + 32 {
        return Err(invalid());
    }
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(db)?;
    mac.update(prefix.as_bytes());
    mac.update(&bytes[..payload]);
    mac.verify_slice(&bytes[payload..]).map_err(|_| invalid())?;
    if bytes[count * 8..payload] != binding[..] {
        return Err(invalid());
    }
    bytes[..count * 8]
        .chunks_exact(8)
        .map(|part| Ok(u64::from_be_bytes(part.try_into().map_err(|_| invalid())?)))
        .collect()
}
pub(super) fn hit_id(binding: &[u8; 32], start: u64, end: u64) -> String {
    let mut hash = Sha256::new();
    hash.update(binding);
    hash.update(start.to_be_bytes());
    hash.update(end.to_be_bytes());
    URL_SAFE_NO_PAD.encode(hash.finalize())
}

pub(super) struct Context {
    pub(super) operation: String,
    pub(super) view: String,
    pub(super) payload: String,
    pub(super) expires: String,
    pub(super) length: u64,
    pub(super) generation: u64,
    pub(super) binding: [u8; 32],
    pub(super) key: Vec<u8>,
    pub(super) query: String,
}
impl Context {
    pub(super) async fn load(
        conn: &mut SqliteConnection,
        principal: &str,
        request: &NoteStageRead,
    ) -> Result<Self> {
        require_workspace(conn, &request.workspace_id).await?;
        let row=sqlx::query("SELECT o.operation_key,o.outcome,s.header_digest,s.payload_digest,s.view_id,s.view_length,s.phase,s.header FROM note_operation o JOIN note_stage s USING(operation_key) WHERE o.principal=? AND o.backend_id=? AND o.workspace_id=? AND o.note_id=? AND o.instance_id=? AND o.operation_id=? AND o.method_kind='staged'")
            .bind(principal).bind(&request.backend_id).bind(&request.workspace_id).bind(&request.note_id).bind(&request.note_instance_id).bind(&request.operation_id).fetch_optional(&mut *conn).await.map_err(db)?.ok_or_else(invalid)?;
        if row.get::<String, _>("header_digest") != request.header_digest {
            return Err(invalid());
        }
        if row.get::<String, _>("phase") != "sealed" {
            return Err(Error::NotePage(NotePageError::Expired));
        }
        let header: NoteStageHeader = serde_json::from_str(row.get("header")).map_err(db)?;
        if header.output != NoteStageOutput::Search {
            return Err(invalid());
        }
        let query = header.query.as_ref().ok_or_else(invalid)?;
        if query.mode != NoteStageSearchMode::Source {
            return Err(Error::Unsupported("rendered search output".into()));
        }
        header.validate().map_err(fail)?;
        let expires = expiry(&serde_json::from_str(row.get("outcome")).map_err(db)?)?;
        let operation: String = row.get("operation_key");
        let view: String = row.get("view_id");
        let payload: String = row.get("payload_digest");
        let length = u64::try_from(row.get::<i64, _>("view_length")).map_err(db)?;
        let generation:i64=sqlx::query_scalar("SELECT generation FROM note_stage_view WHERE operation_key=? ORDER BY generation DESC LIMIT 1").bind(&operation).fetch_one(&mut *conn).await.map_err(db)?;
        let generation = u64::try_from(generation).map_err(db)?;
        let backend =
            sqlx::query("SELECT backend_id,token_key FROM note_page_backend WHERE singleton=1")
                .fetch_one(&mut *conn)
                .await
                .map_err(db)?;
        if backend.get::<String, _>("backend_id") != request.backend_id {
            return Err(invalid());
        }
        let key: Vec<u8> = backend.get("token_key");
        let binding = Sha256::digest(
            serde_json::to_vec(&json!([
                principal,
                request.scope(),
                request.operation_id,
                request.header_digest,
                header,
                operation,
                view,
                payload,
                expires,
                length,
                generation,
                CASE_FOLDING_SHA256
            ]))
            .map_err(db)?,
        )
        .into();
        Ok(Self {
            operation,
            view,
            payload,
            expires,
            length,
            generation,
            binding,
            key,
            query: query.text.clone(),
        })
    }
    pub(super) fn envelope(&self, request: &NoteStageRead, kind: &str) -> Value {
        json!({"kind":"noteOperationPage","scope":request.scope(),"operationId":request.operation_id,"headerDigest":request.header_digest,"payloadDigest":self.payload,"viewId":self.view,"outputKind":kind,"sourceLength":self.length,"items":[],"nextCursor":null,"expiresAt":self.expires})
    }
    pub(super) async fn recheck(&self, store: &Store, request: &NoteStageRead) -> Result<()> {
        let mut conn = store.read_pool().begin().await.map_err(db)?;
        require_workspace(&mut conn, &request.workspace_id).await?;
        let current:Option<(String,String)>=sqlx::query_as("SELECT s.phase,o.outcome FROM note_stage s JOIN note_operation o USING(operation_key) WHERE s.operation_key=? AND s.view_id=? AND s.payload_digest=?")
            .bind(&self.operation).bind(&self.view).bind(&self.payload).fetch_optional(&mut *conn).await.map_err(db)?;
        let Some((phase, raw)) = current else {
            return Err(Error::NotePage(NotePageError::Expired));
        };
        if phase != "sealed" {
            return Err(Error::NotePage(NotePageError::Expired));
        }
        if expiry(&serde_json::from_str(&raw).map_err(db)?)? != self.expires {
            return Err(invalid());
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct Position {
    interval: u64,
    frontier: u64,
    replay: u64,
    count: u64,
}
impl Position {
    fn values(self) -> [u64; 4] {
        [self.interval, self.frontier, self.replay, self.count]
    }
}
fn wire_len(value: &Value, id: &Value) -> usize {
    json!({"jsonrpc":"2.0","id":id,"result":value})
        .to_string()
        .len()
}

/// Restore only the bounded original tail; already emitted matches are ignored.
async fn restore(
    conn: &mut SqliteConnection,
    context: &Context,
    position: Position,
) -> Result<NoteStageSearch> {
    if position.replay > position.frontier || position.frontier - position.replay > 6144 {
        return Err(invalid());
    }
    let mut matcher = NoteStageSearch::new(&context.query).map_err(fail)?;
    matcher.reset_at_gap(position.replay).map_err(fail)?;
    if position.replay == position.frontier {
        return Ok(matcher);
    }
    // A Unicode scalar uses at most three UTF8 bytes per UTF16 unit. This
    // one returned buffer therefore covers the replay, with a fixed 12KiB cap.
    // Count the WHOLE returned buffer, including any suffix beyond the frontier;
    // backing-piece SQL hydration is separately bounded by view_read's pieces.
    let units = usize::try_from(position.frontier - position.replay).map_err(db)?;
    let budget = (units * 3).clamp(4, MAX_CARRY_BYTES);
    let (_, text) = view_read::read_piece(
        conn,
        &context.operation,
        context.generation,
        context.length,
        position.replay,
        budget,
    )
    .await?;
    if text.len() > budget {
        return Err(invalid());
    }
    let mut at = position.replay;
    for scalar in text.chars() {
        if at == position.frontier {
            break;
        }
        let end = at + scalar.len_utf16() as u64;
        if end > position.frontier {
            return Err(invalid());
        }
        matcher.push_scalar(scalar, at).map_err(fail)?;
        at = end;
    }
    if at != position.frontier {
        return Err(invalid());
    }
    Ok(matcher)
}

pub(super) async fn read(
    store: &Store,
    principal: &str,
    request: &NoteStageRead,
    rpc_id: &Value,
) -> Result<Value> {
    let mut tx = store.read_pool().begin().await.map_err(db)?;
    let context = Context::load(&mut tx, principal, request).await?;
    let cursor_binding: [u8; 32] =
        Sha256::digest(serde_json::to_vec(&json!([context.binding, request])).map_err(db)?).into();
    let first = search_ranges::next_interval(
        &mut tx,
        &context.operation,
        context.generation,
        context.length,
        None,
    )
    .await?;
    let mut position = if let Some(token) = &request.cursor {
        let values = verify(token, "nsc1.", &context.key, &cursor_binding, 4)?;
        Position {
            interval: values[0],
            frontier: values[1],
            replay: values[2],
            count: values[3],
        }
    } else {
        Position {
            interval: first.map_or(context.length, |r| r.start),
            frontier: first.map_or(context.length, |r| r.start),
            replay: first.map_or(context.length, |r| r.start),
            count: 0,
        }
    };
    let mut interval = if request.cursor.is_some() {
        let row:Option<(i64,i64)>=sqlx::query_as("SELECT start,end FROM note_stage_search_range WHERE operation_key=? AND generation=? AND start=?")
            .bind(&context.operation).bind(i64::try_from(context.generation).map_err(db)?).bind(i64::try_from(position.interval).map_err(|_|invalid())?).fetch_optional(&mut *tx).await.map_err(db)?;
        let (start, end) = row.ok_or_else(invalid)?;
        Some(search_ranges::SearchInterval {
            start: u64::try_from(start).map_err(db)?,
            end: u64::try_from(end).map_err(db)?,
        })
    } else {
        first
    };
    if position.count > context.length
        || interval.is_some_and(|r| {
            position.replay < r.start
                || position.frontier < position.replay
                || position.frontier > r.end
        })
    {
        return Err(invalid());
    }
    let mut matcher = restore(&mut tx, &context, position).await?;
    let mut result = context.envelope(request, "search");
    let mut items = Vec::new();
    let mut bytes = 0usize;
    let mut transitions = 0;
    let mut exact = interval.is_none();
    'scan: while let Some(current) = interval {
        if position.frontier == current.end {
            interval = search_ranges::next_interval(
                &mut tx,
                &context.operation,
                context.generation,
                context.length,
                Some(current.start),
            )
            .await?;
            let Some(next) = interval else {
                position.frontier = context.length;
                exact = true;
                break;
            };
            position.interval = next.start;
            position.frontier = next.start;
            position.replay = next.start;
            matcher.reset_at_gap(next.start).map_err(fail)?;
            transitions += 1;
            if transitions >= 64 {
                break;
            }
            continue;
        }
        let remaining = request.max_source_bytes.unwrap_or(8192) - bytes;
        if remaining < 4 {
            break;
        }
        let (_, text) = view_read::read_piece(
            &mut tx,
            &context.operation,
            context.generation,
            context.length,
            position.frontier,
            remaining,
        )
        .await?;
        bytes += text.len();
        if text.is_empty() {
            return Err(invalid());
        }
        for scalar in text.chars() {
            if position.frontier == current.end {
                break;
            }
            let previous = position;
            let end = position.frontier + scalar.len_utf16() as u64;
            if end > current.end {
                return Err(invalid());
            }
            let hit = matcher
                .push_scalar(scalar, position.frontier)
                .map_err(fail)?;
            position.frontier = end;
            position.replay = matcher.replay_range().ok_or_else(invalid)?.start;
            if let Some(hit) = hit {
                position.count += 1;
                let item = json!({"hitId":hit_id(&context.binding,hit.start,hit.end),"sourceRange":{"start":hit.start,"end":hit.end},"detailRef":sign("nsh1.",&context.key,&context.binding,&[hit.start,hit.end,0])?});
                items.push(item);
                result["items"] = json!(items);
                // Reserve the largest possible frontier and a full cursor. EOF
                // later only shrinks this frame; first-item overflow is explicit.
                result["scannedThrough"] = json!(context.length);
                result["count"] = json!({"value":position.count,"exact":false});
                result["nextCursor"] = json!(sign(
                    "nsc1.",
                    &context.key,
                    &cursor_binding,
                    &position.values()
                )?);
                if wire_len(&result, rpc_id) > request.max_wire_bytes.unwrap_or(4096) {
                    items.pop();
                    position = previous;
                    if items.is_empty() {
                        return Err(fail(NoteMutationError::Budget));
                    }
                    break 'scan;
                }
                if items.len() == request.max_items.unwrap_or(64) {
                    break 'scan;
                }
            }
        }
    }
    result["items"] = json!(items);
    result["scannedThrough"] = json!(position.frontier);
    result["count"] = json!({"value":position.count,"exact":exact});
    result["nextCursor"] = if exact {
        Value::Null
    } else {
        json!(sign(
            "nsc1.",
            &context.key,
            &cursor_binding,
            &position.values()
        )?)
    };
    if wire_len(&result, rpc_id) > request.max_wire_bytes.unwrap_or(4096) {
        return Err(fail(NoteMutationError::Budget));
    }
    tx.commit().await.map_err(db)?;
    context.recheck(store, request).await?;
    Ok(result)
}

impl Store {
    /// Resolve a signed raw source-hit field from its original sealed view.
    /// Current visibility is owned by Services at both request boundaries.
    /// # Errors
    /// Rejects receipt or cursor fallback, altered offsets/identity, expired views,
    /// unavailable source pieces and complete-frame overflow.
    pub async fn read_note_stage_search_detail(
        &self,
        principal: &str,
        query: &intent_core::note_receipt_detail::ReceiptDetailQuery,
        rpc_id: &Value,
    ) -> Result<Value> {
        use intent_core::{
            note_receipt_detail::ReceiptDetailKind, note_stage_read::NoteStageReadKind,
        };
        query.validate().map_err(fail)?;
        if !query.operation_envelope
            || query.context_envelope
            || query.kind != ReceiptDetailKind::Detail
            || query.payload_digest.is_some()
            || query.cursor.is_some()
            || query.text_id.is_some()
            || principal.is_empty()
            || principal.len() > 256
            || principal.contains('\0')
            || !(rpc_id.as_str().is_some_and(|s| s.len() <= 64)
                || rpc_id
                    .as_i64()
                    .is_some_and(|n| (-9_007_199_254_740_991..=9_007_199_254_740_991).contains(&n)))
        {
            return Err(invalid());
        }
        let request = NoteStageRead {
            backend_id: query.scope.backend_id.clone(),
            workspace_id: query.scope.workspace_id.clone(),
            note_id: query.scope.note_id.clone(),
            note_instance_id: query.scope.note_instance_id.clone(),
            operation_id: query.operation_id.clone(),
            header_digest: query.header_digest.clone().ok_or_else(invalid)?,
            kind: NoteStageReadKind::Search,
            cursor: None,
            max_items: Some(query.max_items),
            max_source_bytes: Some(query.max_source_bytes),
            max_wire_bytes: Some(query.max_wire_bytes),
        };
        request.validate().map_err(fail)?;
        let mut tx = self.read_pool().begin().await.map_err(db)?;
        let context = Context::load(&mut tx, principal, &request).await?;
        let mut page = context.envelope(&request, "detail");
        let item = super::search_detail::read_fragment(
            &mut tx,
            &super::search_detail::SearchDetailRead {
                context: &context,
                reference: &query.reference,
                offset: query.offset,
                max_source_bytes: query.max_source_bytes,
            },
            |item| {
                page["items"] = json!([item]);
                Ok(wire_len(&page, rpc_id) <= query.max_wire_bytes)
            },
        )
        .await?;
        page["items"] = json!([item]);
        tx.commit().await.map_err(db)?;
        context.recheck(self, &request).await?;
        Ok(page)
    }
}
