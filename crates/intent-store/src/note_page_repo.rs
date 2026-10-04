//! Bounded reads over write-maintained indexes. Tokens are authenticated with a
//! persistent database secret, while bounded snapshot leases are process-local.
mod artifact;
mod artifact_append;
pub use artifact_append::ArtifactJournalRecordCost;
mod artifact_begin;
mod token;
pub use artifact::ArtifactSourceGrant;
mod artifact_lifecycle;
mod artifact_publication;
use crate::Store;
pub use artifact_lifecycle::ArtifactJournalStatus;
pub use artifact_publication::ArtifactJournalLease;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use hmac::{Hmac, Mac};
use intent_core::{
    note_page::{NotePageError, NotePageRequest, NoteScope},
    Error, Result,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::Sha256;
use sqlx::{Row, SqliteConnection, SqlitePool};
use std::{collections::BTreeMap, sync::Mutex, time::Instant};

const MAX_SNAPSHOTS: usize = 256;
const SNAPSHOT_SECONDS: u64 = 300;

#[derive(Clone)]
struct Snapshot {
    scope: NoteScope,
    revision: String,
    generation: String,
    principal: String,
    born: Instant,
    expires: String,
}

#[cfg_attr(test, derive(Default))]
pub(crate) struct Runtime {
    id: String,
    backend: String,
    key: Vec<u8>,
    snapshots: Mutex<BTreeMap<String, Snapshot>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Token(String, String, String, u64, usize, usize, usize);

fn failure(kind: NotePageError) -> Error {
    Error::NotePage(kind)
}
fn invalid() -> Error {
    Error::InvalidParams("Invalid note page request".into())
}
#[expect(clippy::needless_pass_by_value)] // Result::map_err transfers ownership.
fn db_error(e: sqlx::Error) -> Error {
    Error::Internal(format!("note page query: {e}"))
}
fn unsigned(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::Internal("negative note index offset".into()))
}
fn signed(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| invalid())
}
fn wire_len(value: &Value, id: &Value) -> usize {
    json!({"jsonrpc":"2.0","id":id,"result":value})
        .to_string()
        .len()
}
fn utf16_byte(text: &str, offset: u64) -> Result<usize> {
    let mut at = 0;
    for (byte, ch) in text.char_indices() {
        if at == offset {
            return Ok(byte);
        }
        at += ch.len_utf16() as u64;
        if at > offset {
            return Err(invalid());
        }
    }
    if at == offset {
        Ok(text.len())
    } else {
        Err(invalid())
    }
}

impl Runtime {
    pub(crate) async fn open(pool: &SqlitePool) -> Result<Self> {
        let row =
            sqlx::query("SELECT backend_id,token_key FROM note_page_backend WHERE singleton=1")
                .fetch_one(pool)
                .await
                .map_err(db_error)?;
        Ok(Self {
            id: uuid::Uuid::new_v4().simple().to_string(),
            backend: row.try_get("backend_id").map_err(db_error)?,
            key: row.try_get("token_key").map_err(db_error)?,
            snapshots: Mutex::new(BTreeMap::new()),
        })
    }
    fn token(&self, value: &Token) -> String {
        let bytes = token::encode(value);
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.key).expect("HMAC accepts any key length");
        mac.update(&bytes);
        format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(&bytes),
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
        )
    }
    fn decode(&self, text: &str) -> Result<Token> {
        if text.len() > 256 {
            return Err(failure(NotePageError::CursorInvalid));
        }
        let (data, signature) = text
            .split_once('.')
            .ok_or_else(|| failure(NotePageError::CursorInvalid))?;
        let data = URL_SAFE_NO_PAD
            .decode(data)
            .map_err(|_| failure(NotePageError::CursorInvalid))?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| failure(NotePageError::CursorInvalid))?;
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.key).expect("HMAC accepts any key length");
        mac.update(&data);
        mac.verify_slice(&signature)
            .map_err(|_| failure(NotePageError::CursorInvalid))?;
        if data.first() == Some(&b'[') {
            serde_json::from_slice(&data).map_err(|_| failure(NotePageError::CursorInvalid))
        } else {
            token::decode(&data).ok_or_else(|| failure(NotePageError::CursorInvalid))
        }
    }
    fn snapshot(&self, id: &str, ws: &str, note: &str, principal: &str) -> Result<Snapshot> {
        let snapshots = self
            .snapshots
            .lock()
            .map_err(|_| Error::Internal("note snapshots poisoned".into()))?;
        let snapshot = snapshots
            .get(id)
            .ok_or_else(|| failure(NotePageError::Expired))?;
        if snapshot.scope.workspace_id != ws
            || snapshot.scope.note_id != note
            || snapshot.principal != principal
        {
            return Err(failure(NotePageError::CursorInvalid));
        }
        if snapshot.born.elapsed().as_secs() >= SNAPSHOT_SECONDS {
            return Err(failure(NotePageError::Expired));
        }
        Ok(snapshot.clone())
    }
    fn remember(&self, snapshot: Snapshot) -> Result<String> {
        let mut snapshots = self
            .snapshots
            .lock()
            .map_err(|_| Error::Internal("note snapshots poisoned".into()))?;
        snapshots.retain(|_, s| s.born.elapsed().as_secs() < SNAPSHOT_SECONDS);
        if snapshots.len() >= MAX_SNAPSHOTS {
            if let Some(id) = snapshots
                .iter()
                .min_by_key(|(_, s)| s.born)
                .map(|(id, _)| id.clone())
            {
                snapshots.remove(&id);
            }
        }
        let id = uuid::Uuid::new_v4().simple().to_string();
        snapshots.insert(id.clone(), snapshot);
        Ok(id)
    }
    fn reference(&self, snapshot: &str, raw: &str) -> String {
        let (collection, offset) = raw.split_once('@').map_or((raw, 0), |(c, n)| {
            (c, n.parse().expect("internal fragment offset"))
        });
        self.token(&Token(
            snapshot.into(),
            "r".into(),
            collection.into(),
            offset,
            0,
            0,
            0,
        ))
    }
    fn references(&self, snapshot: &str, value: &mut Value) {
        if let Some(map) = value.as_object_mut() {
            for (key, value) in map {
                if key.ends_with("Ref") {
                    if let Some(raw) = value.as_str() {
                        *value = json!(self.reference(snapshot, raw));
                    }
                }
            }
        }
    }
}

impl Store {
    /// Read indexed source or descriptor rows at a single `SQLite` read snapshot.
    /// Authorization is checked by Services before entering this repository.
    ///
    /// # Errors
    /// Returns typed page errors for stale, expired or invalid continuations.
    pub async fn read_note_page(
        &self,
        ws: &str,
        note: &str,
        principal: &str,
        request: NotePageRequest,
        rpc_id: &Value,
    ) -> Result<Value> {
        let source_bytes = request.max_source_bytes.unwrap_or(16384);
        let wire_bytes = request.max_wire_bytes.unwrap_or(65536);
        let max_items = request.max_items.unwrap_or(128);
        if !(4..=16384).contains(&source_bytes)
            || !(4096..=65536).contains(&wire_bytes)
            || !(1..=128).contains(&max_items)
        {
            return Err(failure(NotePageError::Budget));
        }
        if ws.is_empty() || note.is_empty() || ws.len() > 256 || note.len() > 256 {
            return Err(invalid());
        }
        for text in [
            &request.cursor,
            &request.snapshot_id,
            &request.source_revision,
            &request.note_instance_id,
            &request.context_ref,
            &request.reference,
        ]
        .into_iter()
        .flatten()
        {
            if text.len() > 256 || text.is_empty() {
                return Err(invalid());
            }
        }
        if request.at.is_some_and(|n| n > 9_007_199_254_740_991) {
            return Err(invalid());
        }
        let source = request.kind == "source";
        let tasks = request.kind == "taskIds";
        if !source && !matches!(request.kind.as_str(), "context" | "metadata" | "taskIds") {
            return Err(invalid());
        }
        if request
            .direction
            .as_deref()
            .is_some_and(|v| !matches!(v, "forward" | "backward"))
        {
            return Err(invalid());
        }
        if (source || tasks) && (request.context_ref.is_some() || request.reference.is_some()) {
            return Err(invalid());
        }
        if !source
            && (request.at.is_some()
                || request.direction.is_some()
                || (!tasks
                    && (request.snapshot_id.is_some()
                        || request.source_revision.is_some()
                        || request.note_instance_id.is_some()))
                || request.max_source_bytes.is_some())
        {
            return Err(invalid());
        }
        if (source || tasks)
            && request.cursor.is_some()
            && (request.at.is_some()
                || request.direction.is_some()
                || request.snapshot_id.is_some()
                || request.source_revision.is_some()
                || request.note_instance_id.is_some())
        {
            return Err(invalid());
        }
        let reference = if request.kind == "context" {
            if request.reference.is_some() {
                return Err(invalid());
            }
            request.context_ref.as_deref()
        } else if request.kind == "metadata" {
            if request.context_ref.is_some() {
                return Err(invalid());
            }
            request.reference.as_deref()
        } else {
            None
        };
        if !source && !tasks && reference.is_none() {
            return Err(invalid());
        }
        let ref_token = reference.map(|r| self.note_pages.decode(r)).transpose()?;
        if ref_token.as_ref().is_some_and(|t| {
            t.1 != "r"
                || if request.kind == "metadata" {
                    !(t.2.starts_with("m:") || t.2.starts_with("a:"))
                } else {
                    t.2.starts_with("m:") || t.2.starts_with("a:")
                }
        }) {
            return Err(failure(NotePageError::CursorInvalid));
        }
        let cursor = request
            .cursor
            .as_deref()
            .map(|r| self.note_pages.decode(r))
            .transpose()?;
        if let Some(token) = &cursor {
            if token.4 != source_bytes
                || token.5 != wire_bytes
                || token.6 != max_items
                || if source {
                    !matches!(token.1.as_str(), "s+" | "s-")
                } else {
                    token.1 != request.kind
                }
            {
                return Err(failure(NotePageError::CursorInvalid));
            }
            if ref_token
                .as_ref()
                .is_some_and(|r| r.0 != token.0 || r.2 != token.2)
            {
                return Err(failure(NotePageError::CursorInvalid));
            }
        }
        let snapshot_id = cursor
            .as_ref()
            .or(ref_token.as_ref())
            .map(|t| t.0.as_str())
            .or(request.snapshot_id.as_deref());
        let snapshot = snapshot_id
            .map(|id| self.note_pages.snapshot(id, ws, note, principal))
            .transpose()?;
        if request.snapshot_id.is_some()
            && (request.source_revision.is_none() || request.note_instance_id.is_none())
        {
            return Err(invalid());
        }
        let mut tx = self.read_pool().begin().await.map_err(db_error)?;
        let head = sqlx::query("SELECT instance_id,indexed_rev,profile_revision,source_length,task_count,current_rev AS rev,generation FROM note_page_head WHERE workspace_id=? AND note_id=?")
            .bind(ws).bind(note).fetch_optional(&mut *tx).await.map_err(db_error)?.ok_or_else(||Error::NotFound("Note not found".into()))?;
        if head
            .try_get::<String, _>("profile_revision")
            .map_err(db_error)?
            != crate::note_page_index::profile_revision()
        {
            return Err(failure(NotePageError::Expired));
        }
        let instance: String = head.try_get("instance_id").map_err(db_error)?;
        let rev: i64 = head.try_get("rev").map_err(db_error)?;
        let generation: String = head.try_get("generation").map_err(db_error)?;
        // Legacy SQL parent rewrites can change metadata without advancing rev.
        // Include their persistent index generation in the opaque read revision.
        let revision = format!("r:{rev}:{generation}");
        if snapshot.as_ref().is_some_and(|s| {
            s.revision != revision
                || s.scope.note_instance_id != instance
                || s.generation != generation
        }) || request
            .source_revision
            .as_ref()
            .is_some_and(|v| v != &revision)
            || request
                .note_instance_id
                .as_ref()
                .is_some_and(|v| v != &instance)
        {
            return Err(failure(NotePageError::Stale));
        }
        if head.try_get::<i64, _>("indexed_rev").map_err(db_error)? != rev {
            return Err(failure(NotePageError::Expired));
        }
        let snapshot = snapshot.unwrap_or_else(|| Snapshot {
            scope: NoteScope {
                backend_id: self.note_pages.backend.clone(),
                workspace_id: ws.into(),
                note_id: note.into(),
                note_instance_id: instance,
            },
            revision,
            generation,
            principal: principal.into(),
            born: Instant::now(),
            expires: intent_core::iso_ms_from_now(300_000),
        });
        let snapshot_id = match snapshot_id {
            Some(id) => id.into(),
            None => self.note_pages.remember(snapshot.clone())?,
        };
        let mut base = json!({"scope":snapshot.scope,"sourceRevision":snapshot.revision,"snapshotId":snapshot_id,"expiresAt":snapshot.expires});
        let result = if source {
            let length: i64 = head.try_get("source_length").map_err(db_error)?;
            let backward = cursor
                .as_ref()
                .map_or(request.direction.as_deref() == Some("backward"), |t| {
                    t.1 == "s-"
                });
            let at = cursor
                .as_ref()
                .map(|t| t.3)
                .or(request.at)
                .unwrap_or(if backward { unsigned(length)? } else { 0 });
            self.source_page(
                &mut tx,
                ws,
                note,
                base,
                at,
                unsigned(length)?,
                backward,
                source_bytes,
                wire_bytes,
                max_items,
                rpc_id,
            )
            .await?
        } else {
            let initial_tasks = Token(
                snapshot_id.clone(),
                "taskIds".into(),
                "t:root".into(),
                0,
                16384,
                wire_bytes,
                max_items,
            );
            if tasks {
                base["totalItems"] =
                    json!(head.try_get::<i64, _>("task_count").map_err(db_error)?);
            }
            let token = cursor
                .as_ref()
                .or(ref_token.as_ref())
                .unwrap_or(&initial_tasks);
            self.entry_page(
                &mut tx,
                ws,
                note,
                base,
                token,
                &request.kind,
                wire_bytes,
                max_items,
                rpc_id,
            )
            .await?
        };
        tx.commit().await.map_err(db_error)?;
        Ok(result)
    }

    #[expect(clippy::too_many_arguments)] // Explicit immutable request and read snapshot fields.
    async fn source_page(
        &self,
        conn: &mut SqliteConnection,
        ws: &str,
        note: &str,
        mut base: Value,
        at: u64,
        length: u64,
        backward: bool,
        source_bytes: usize,
        wire_bytes: usize,
        max_items: usize,
        rpc_id: &Value,
    ) -> Result<Value> {
        if at > length {
            return Err(invalid());
        }
        // At a backward seam choose the preceding piece; forward chooses the next.
        let comparison = if backward && at > 0 { "<" } else { "<=" };
        let sql=format!("SELECT start,end,text FROM note_page_piece WHERE workspace_id=? AND note_id=? AND start {comparison} ? ORDER BY start DESC LIMIT 1");
        let row = sqlx::query(&sql)
            .bind(ws)
            .bind(note)
            .bind(signed(at)?)
            .fetch_one(conn)
            .await
            .map_err(db_error)?;
        let piece_start: i64 = row.try_get("start").map_err(db_error)?;
        let text: String = row.try_get("text").map_err(db_error)?;
        let byte = utf16_byte(&text, at - unsigned(piece_start)?)?;
        let candidate = if backward {
            &text[..byte]
        } else {
            &text[byte..]
        };
        let mut selected = String::new();
        if backward {
            let mut bytes = 0;
            for ch in candidate.chars().rev() {
                if bytes + ch.len_utf8() > source_bytes {
                    break;
                }
                bytes += ch.len_utf8();
            }
            selected.push_str(&candidate[candidate.len() - bytes..]);
        } else {
            for ch in candidate.chars() {
                if selected.len() + ch.len_utf8() > source_bytes {
                    break;
                }
                selected.push(ch);
            }
        }
        let sid = base["snapshotId"].as_str().expect("snapshot id").to_owned();
        base["kind"] = json!("noteSourcePage");
        base["sourceLength"] = json!(length);
        base["metadataRef"] = json!(self.note_pages.reference(&sid, "m:root"));
        loop {
            let units = selected.encode_utf16().count() as u64;
            let (start, end) = if backward {
                (at - units, at)
            } else {
                (at, at + units)
            };
            base["contextRef"] = json!(self
                .note_pages
                .reference(&sid, &format!("c:{piece_start}:{start}:{end}")));
            base["range"] = json!({"start":start,"end":end});
            base["text"] = json!(selected);
            let continuation = |offset, kind: &str| {
                self.note_pages.token(&Token(
                    sid.clone(),
                    kind.into(),
                    String::new(),
                    offset,
                    source_bytes,
                    wire_bytes,
                    max_items,
                ))
            };
            base["nextCursor"] = if end == length {
                Value::Null
            } else {
                json!(continuation(end, "s+"))
            };
            base["previousCursor"] = if start == 0 {
                Value::Null
            } else {
                json!(continuation(start, "s-"))
            };
            if wire_len(&base, rpc_id) <= wire_bytes {
                return Ok(base);
            }
            if selected.chars().count() <= 1 {
                return Err(failure(NotePageError::Budget));
            }
            let keep = selected.chars().count() / 2;
            selected = if backward {
                selected
                    .chars()
                    .skip(selected.chars().count() - keep)
                    .collect()
            } else {
                selected.chars().take(keep).collect()
            };
        }
    }

    #[expect(clippy::too_many_arguments)] // Explicit immutable request and read snapshot fields.
    async fn entry_page(
        &self,
        conn: &mut SqliteConnection,
        ws: &str,
        note: &str,
        mut base: Value,
        token: &Token,
        kind: &str,
        wire_bytes: usize,
        max_items: usize,
        rpc_id: &Value,
    ) -> Result<Value> {
        let sid = base["snapshotId"].as_str().expect("snapshot id").to_owned();
        base["kind"] = json!(if kind == "metadata" {
            "noteMetadataPage"
        } else if kind == "taskIds" {
            "noteTaskIdsPage"
        } else {
            "noteContextPage"
        });
        if kind == "taskIds" {
            base["startIndex"] = json!(token.3);
        }
        base["items"] = json!([]);
        base["nextCursor"] = Value::Null;
        let fragment = token.2.starts_with("f:");
        let context_range: Vec<_> = token.2.split(':').collect();
        let window = matches!(context_range.first().copied(), Some("c" | "d" | "h"))
            && context_range.len() == 4;
        let context_window = window && context_range[0] == "c";
        let map_window = window && context_range[0] == "h";
        let sql = if fragment {
            "SELECT position,value FROM note_page_entry WHERE workspace_id=? AND note_id=? AND collection=? AND position<=? ORDER BY position DESC LIMIT 1"
        } else if context_window {
            "SELECT position,value FROM note_page_entry WHERE workspace_id=? AND note_id=? AND collection=? AND position>=? AND ((source_start<? AND source_end>?) OR (source_start=source_end AND source_start>=? AND source_start<=?)) ORDER BY position LIMIT ?"
        } else if map_window {
            // Identity/omitted runs need positive half-open overlap. Keep the
            // separately indexed projection and non-bijective seam candidates,
            // but never turn an endpoint-only identity into an empty wire map.
            "SELECT position,value FROM note_page_entry WHERE workspace_id=? AND note_id=? AND collection=? AND position>=? AND position<=? AND ((source_start<? AND source_end>?) OR json_extract(value,'$.mapping') NOT IN ('identity','omitted')) ORDER BY position LIMIT ?"
        } else {
            "SELECT position,value FROM note_page_entry WHERE workspace_id=? AND note_id=? AND collection=? AND position>=? ORDER BY position LIMIT ?"
        };
        let clip_fragment = fragment && context_range.len() == 4;
        let bounds = if window || clip_fragment {
            Some((
                context_range[2]
                    .parse::<u64>()
                    .map_err(|_| failure(NotePageError::CursorInvalid))?,
                context_range[3]
                    .parse::<u64>()
                    .map_err(|_| failure(NotePageError::CursorInvalid))?,
            ))
        } else {
            None
        };
        let collection = if window || clip_fragment {
            format!("{}:{}", context_range[0], context_range[1])
        } else {
            token.2.clone()
        };
        let mut first = token.3;
        let mut last = u64::MAX;
        if map_window {
            let (start, end) = bounds.expect("map bounds");
            let range=sqlx::query("SELECT (SELECT position FROM note_page_entry WHERE workspace_id=? AND note_id=? AND collection=? AND source_end>=? ORDER BY source_end,position LIMIT 1) AS first, (SELECT position FROM note_page_entry WHERE workspace_id=? AND note_id=? AND collection=? AND source_start<=? ORDER BY source_start DESC,position DESC LIMIT 1) AS last")
                .bind(ws).bind(note).bind(&collection).bind(signed(start)?).bind(ws).bind(note).bind(&collection).bind(signed(end)?).fetch_one(&mut *conn).await.map_err(db_error)?;
            let (Some(begin), Some(end)) = (
                range.try_get::<Option<i64>, _>("first").map_err(db_error)?,
                range.try_get::<Option<i64>, _>("last").map_err(db_error)?,
            ) else {
                return Ok(base);
            };
            first = first.max(unsigned(begin)?);
            last = unsigned(end)?;
            if first > last {
                return Ok(base);
            }
        }
        let query_position = if clip_fragment {
            first + bounds.expect("fragment bounds").0
        } else {
            first
        };
        let mut query = sqlx::query(sql)
            .bind(ws)
            .bind(note)
            .bind(&collection)
            .bind(signed(query_position)?);
        if context_window {
            let (start, end) = bounds.expect("context bounds");
            query = query
                .bind(signed(end)?)
                .bind(signed(start)?)
                .bind(signed(start)?)
                .bind(signed(end)?);
        }
        if map_window {
            let (start, end) = bounds.expect("map bounds");
            query = query
                .bind(signed(last)?)
                .bind(signed(end)?)
                .bind(signed(start)?);
        }
        if !fragment {
            query = query.bind(i64::try_from(max_items + 1).expect("validated item budget"));
        }
        let mut rows = query.fetch_all(conn).await.map_err(db_error)?;
        if map_window {
            rows.retain(|row| {
                u64::try_from(row.get::<i64, _>("position")).is_ok_and(|position| position <= last)
            });
        }
        if fragment {
            let row = rows
                .first()
                .ok_or_else(|| failure(NotePageError::CursorInvalid))?;
            let raw: String = row.try_get("value").map_err(db_error)?;
            let mut item: Value = serde_json::from_str(&raw)
                .map_err(|_| Error::Internal("note fragment index".into()))?;
            let start: i64 = row.try_get("position").map_err(db_error)?;
            let full = item["text"].as_str().expect("indexed fragment");
            let byte = utf16_byte(full, query_position - unsigned(start)?)?;
            let full_end = unsigned(start)? + full.encode_utf16().count() as u64;
            let (clip_start, clip_end) = if clip_fragment {
                bounds.expect("clip bounds")
            } else {
                (0, full_end)
            };
            let actual_end = full_end.min(clip_end);
            let last_byte = utf16_byte(full, actual_end - unsigned(start)?)?;
            let mut text = full[byte..last_byte].to_owned();
            let end = actual_end - clip_start;
            let tail = if clip_fragment {
                (full_end < clip_end).then(|| format!("{}@{}", token.2, full_end - clip_start))
            } else {
                item["nextRef"].as_str().map(str::to_owned)
            };
            loop {
                let next = token.3 + text.encode_utf16().count() as u64;
                item["offset"] = json!(token.3);
                item["text"] = json!(text);
                let next_ref = if next < end {
                    Some(format!("{}@{next}", token.2))
                } else {
                    tail.clone()
                };
                item["nextRef"] = next_ref
                    .as_deref()
                    .map_or(Value::Null, |r| json!(self.note_pages.reference(&sid, r)));
                base["items"] = json!([item]);
                base["nextCursor"] = next_ref.map_or(Value::Null, |_| {
                    json!(self.note_pages.token(&Token(
                        sid.clone(),
                        kind.into(),
                        token.2.clone(),
                        next,
                        16384,
                        wire_bytes,
                        max_items
                    )))
                });
                if wire_len(&base, rpc_id) <= wire_bytes {
                    return Ok(base);
                }
                if text.chars().count() <= 1 {
                    return Err(failure(NotePageError::Budget));
                }
                text = text.chars().take(text.chars().count() / 2).collect();
            }
        }
        let mut items = Vec::new();
        for (i, row) in rows.iter().enumerate() {
            let position: i64 = row.try_get("position").map_err(db_error)?;
            if i == max_items {
                base["nextCursor"] = json!(self.note_pages.token(&Token(
                    sid.clone(),
                    kind.into(),
                    token.2.clone(),
                    unsigned(position)?,
                    16384,
                    wire_bytes,
                    max_items
                )));
                break;
            }
            let raw: String = row.try_get("value").map_err(db_error)?;
            let mut item: Value = serde_json::from_str(&raw)
                .map_err(|_| Error::Internal("note entry index".into()))?;
            if item["kind"] == "boundary" && window {
                let start = context_range[2]
                    .parse::<u64>()
                    .expect("authenticated source range");
                let end = context_range[3]
                    .parse::<u64>()
                    .expect("authenticated source range");
                item["continuationBefore"] =
                    json!(item["sourceRange"]["start"].as_u64().expect("index range") < start);
                item["continuationAfter"] =
                    json!(item["sourceRange"]["end"].as_u64().expect("index range") > end);
            }
            if window
                && matches!(item["kind"].as_str(), Some("boundary" | "span"))
                && item.get("htmlPosition").is_none()
                && item.get("codeSource").is_none()
            {
                if let Some(parent) = item["parentRef"].as_str() {
                    item["parentRef"] = json!(format!(
                        "{parent}:{}:{}",
                        context_range[2], context_range[3]
                    ));
                }
            }
            if window {
                if let Some(raw) = item["sourceMapRef"].as_str() {
                    item["sourceMapRef"] =
                        json!(format!("{raw}:{}:{}", context_range[2], context_range[3]));
                }
            }
            if map_window && matches!(item["mapping"].as_str(), Some("identity" | "omitted")) {
                let (start, end) = bounds.expect("map bounds");
                let raw_start = item["sourceRange"]["start"].as_u64().expect("map start");
                let raw_end = item["sourceRange"]["end"].as_u64().expect("map end");
                let clipped_start = raw_start.max(start).min(raw_end);
                let clipped_end = raw_end.min(end).max(clipped_start);
                item["sourceRange"] = json!({"start":clipped_start,"end":clipped_end});
                if item["mapping"] == "identity" {
                    let local_start = clipped_start - raw_start;
                    let local_end = clipped_end - raw_start;
                    let rendered_start = item["renderedRange"]["start"]
                        .as_u64()
                        .expect("rendered start");
                    item["renderedRange"] =
                        json!({"start":rendered_start+local_start,"end":rendered_start+local_end});
                    debug_assert!(local_start < local_end, "identity SQL overlap is positive");
                    if let Some(reference) = item["textRef"].as_str() {
                        item["textRef"] = json!(format!("{reference}:{local_start}:{local_end}"));
                    }
                }
            }
            for field in ["htmlPosition", "htmlSource"] {
                if let Some(value) = item.get_mut(field) {
                    self.note_pages.references(&sid, value);
                }
            }
            if let Some(position) = item.get_mut("tablePosition") {
                if window {
                    if let Some(table) = position["tableRef"].as_str() {
                        position["tableRef"] =
                            json!(format!("{table}:{}:{}", context_range[2], context_range[3]));
                    }
                }
                self.note_pages.references(&sid, position);
            }
            self.note_pages.references(&sid, &mut item);
            items.push(item);
            base["items"] = json!(items);
            base["nextCursor"] = if i + 1 < rows.len() {
                json!(self.note_pages.token(&Token(
                    sid.clone(),
                    kind.into(),
                    token.2.clone(),
                    unsigned(position)? + 1,
                    16384,
                    wire_bytes,
                    max_items
                )))
            } else {
                Value::Null
            };
            if wire_len(&base, rpc_id) > wire_bytes {
                items.pop();
                if items.is_empty() {
                    return Err(failure(NotePageError::Budget));
                }
                base["items"] = json!(items);
                base["nextCursor"] = json!(self.note_pages.token(&Token(
                    sid.clone(),
                    kind.into(),
                    token.2.clone(),
                    unsigned(position)?,
                    16384,
                    wire_bytes,
                    max_items
                )));
                break;
            }
        }
        if wire_len(&base, rpc_id) > wire_bytes {
            return Err(failure(NotePageError::Budget));
        }
        Ok(base)
    }
}

#[cfg(test)]
mod lifetime_tests {
    use super::*;
    #[tokio::test]
    async fn authenticated_snapshot_expiry_eviction_and_principal_scope() {
        let runtime = Runtime {
            id: "test-runtime".into(),
            backend: "db".into(),
            key: vec![7; 32],
            snapshots: Mutex::new(BTreeMap::new()),
        };
        let snapshot = Snapshot {
            scope: NoteScope {
                backend_id: "db".into(),
                workspace_id: "ws".into(),
                note_id: "n".into(),
                note_instance_id: "inc".into(),
            },
            revision: "r:0".into(),
            generation: "g".into(),
            principal: "alice".into(),
            born: Instant::now(),
            expires: "fixed".into(),
        };
        let id = runtime.remember(snapshot.clone()).unwrap();
        let value = Token(
            id.clone(),
            "r".into(),
            "d:0000000000000001".into(),
            0,
            0,
            0,
            0,
        );
        let legacy_bytes = serde_json::to_vec(&value).unwrap();
        let mut legacy_mac = Hmac::<Sha256>::new_from_slice(&runtime.key).unwrap();
        legacy_mac.update(&legacy_bytes);
        let legacy = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(&legacy_bytes),
            URL_SAFE_NO_PAD.encode(legacy_mac.finalize().into_bytes())
        );
        let compact = runtime.token(&value);
        for encoded in [&legacy, &compact] {
            let token = runtime.decode(encoded).unwrap();
            for (ws, note, principal) in [
                ("other", "n", "alice"),
                ("ws", "other", "alice"),
                ("ws", "n", "bob"),
            ] {
                assert!(matches!(
                    runtime.snapshot(&token.0, ws, note, principal),
                    Err(Error::NotePage(NotePageError::CursorInvalid))
                ));
            }
            let restarted = Runtime {
                id: "restarted-runtime".into(),
                backend: runtime.backend.clone(),
                key: runtime.key.clone(),
                snapshots: Mutex::new(BTreeMap::new()),
            };
            let restarted_token = restarted.decode(encoded).unwrap();
            assert!(matches!(
                restarted.snapshot(&restarted_token.0, "ws", "n", "alice"),
                Err(Error::NotePage(NotePageError::Expired))
            ));
        }
        assert!(matches!(
            runtime.snapshot(&id, "ws", "n", "bob"),
            Err(Error::NotePage(NotePageError::CursorInvalid))
        ));
        runtime.snapshots.lock().unwrap().get_mut(&id).unwrap().born = Instant::now()
            .checked_sub(std::time::Duration::from_secs(301))
            .unwrap();
        assert!(matches!(
            runtime.snapshot(&id, "ws", "n", "alice"),
            Err(Error::NotePage(NotePageError::Expired))
        ));
        for encoded in [&legacy, &compact] {
            let token = runtime.decode(encoded).unwrap();
            assert!(matches!(
                runtime.snapshot(&token.0, "ws", "n", "alice"),
                Err(Error::NotePage(NotePageError::Expired))
            ));
        }
        let id = runtime.remember(snapshot.clone()).unwrap();
        runtime.snapshots.lock().unwrap().get_mut(&id).unwrap().born = Instant::now()
            .checked_sub(std::time::Duration::from_secs(1))
            .unwrap();
        for _ in 0..MAX_SNAPSHOTS {
            runtime.remember(snapshot.clone()).unwrap();
        }
        assert_eq!(runtime.snapshots.lock().unwrap().len(), MAX_SNAPSHOTS);
        assert!(matches!(
            runtime.snapshot(&id, "ws", "n", "alice"),
            Err(Error::NotePage(NotePageError::Expired))
        ));
    }
}
