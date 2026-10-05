use super::{
    bad_cursor, budget, failure, invalid, text_id, validate_rpc_id, wire_len, AnchorFilter,
    AnnotationKind, AnnotationPageRequest, Lease, Query, Token,
};
use crate::{
    note_annotation_repo::{db_error, AnnotationEpochs, CommentFilter, SourceRange},
    Store,
};
use intent_core::{
    note_page::{NotePageError, NoteScope},
    NoteId, Result, WorkspaceId,
};
use serde_json::{json, Value};
use sqlx::Row;
use uuid::Uuid;

pub(super) fn number(n: i64) -> Result<u64> {
    u64::try_from(n).map_err(|_| invalid())
}
pub(super) fn integer(n: u64) -> Result<i64> {
    i64::try_from(n).map_err(|_| invalid())
}
pub(super) fn trim(text: &str, max: usize) -> &str {
    let mut end = max.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}
pub(super) fn reference(id: Uuid, kind: u8, owner: u64, key: &[u8]) -> Result<String> {
    Token {
        snapshot: id,
        kind,
        owner,
        position: 0,
        utf16: 0,
        items: 0,
        wire: 0,
        binding: [0; 16],
    }
    .encode(key)
}
pub(super) fn envelope(lease: &Lease, id: Uuid, kind: &str) -> Value {
    let mut out = json!({"kind":kind,"scope":lease.scope,"sourceRevision":lease.source_revision,"snapshotId":id.simple().to_string(),"expiresAt":lease.expires_at,"items":[],"nextCursor":null});
    let field = if lease.query.kind == AnnotationKind::Attribution {
        "attributionGeneration"
    } else {
        "commentRevision"
    };
    out[field] = json!(lease.epoch);
    out
}

impl Store {
    /// Read an authenticated, bounded annotation page after service authorization.
    /// The service supplies the actual JSON-RPC ID for complete-frame budgeting.
    ///
    /// # Errors
    /// Rejects invalid shapes, stale epochs, expired/tampered cursors, or a frame
    /// whose first item cannot fit. Never falls back to a legacy full response.
    #[expect(clippy::too_many_arguments)]
    pub async fn read_note_annotation_page(
        &self,
        principal: &str,
        scope: &NoteScope,
        source_revision: &str,
        epoch: Option<&str>,
        thread_id: Option<&str>,
        page: &AnnotationPageRequest,
        rpc_id: &Value,
    ) -> Result<Value> {
        validate_rpc_id(rpc_id)?;
        if ![
            scope.backend_id.as_str(),
            scope.workspace_id.as_str(),
            scope.note_id.as_str(),
            scope.note_instance_id.as_str(),
            source_revision,
        ]
        .into_iter()
        .all(text_id)
            || epoch.is_some_and(|e| !text_id(e))
        {
            return Err(invalid());
        }
        let query = Query::admit(page, thread_id)?;
        let key = self.annotation_key().await?;
        let ws = WorkspaceId::from(scope.workspace_id.as_str());
        let note = NoteId::from(scope.note_id.as_str());
        let (id, lease, cursor) = if let Some(cursor) = &page.cursor {
            let cursor = Token::decode(cursor, &key)?;
            let lease = self.annotation_lease(cursor.snapshot).await?;
            if lease.query != query
                || cursor.kind != query.kind_tag()
                || cursor.items != u16::try_from(query.items).map_err(|_| invalid())?
                || cursor.wire != u32::try_from(query.wire).map_err(|_| invalid())?
                || lease.source_revision != source_revision
                || epoch != Some(lease.epoch.as_str())
            {
                return Err(bad_cursor());
            }
            (cursor.snapshot, lease, Some(cursor))
        } else {
            let state = self
                .read_note_page_state(&ws, &note, Some(&scope.note_instance_id))
                .await?;
            if state["scope"] != serde_json::to_value(scope).map_err(|_| invalid())? {
                return Err(bad_cursor());
            }
            if state["deleted"] == true || state["sourceRevision"] != source_revision {
                return Err(failure(NotePageError::Stale));
            }
            let name = if query.kind == AnnotationKind::Attribution {
                "attributionGeneration"
            } else {
                "commentRevision"
            };
            let current = state[name].as_str().ok_or_else(invalid)?;
            if epoch.is_some_and(|epoch| epoch != current) {
                return Err(failure(NotePageError::Stale));
            }
            let lease = Lease {
                scope: scope.clone(),
                principal: principal.to_owned(),
                source_revision: source_revision.to_owned(),
                epoch: current.to_owned(),
                query,
                expires_at: intent_core::iso_ms_from_now(300_000),
                expires_ms: i64::try_from(intent_core::now_epoch_ms()).map_err(|_| invalid())?
                    + 300_000,
            };
            let id = self.save_annotation_lease(&lease).await?;
            (id, lease, None)
        };
        let epochs = self
            .validate_annotation_lease(&lease, scope, principal)
            .await?;
        let (mut out, rows, more) = self
            .annotation_summary_rows(&lease, id, &epochs, cursor.as_ref(), &key)
            .await?;
        let mut admitted = Vec::new();
        for (index, (item, owner, position)) in rows.iter().enumerate() {
            admitted.push(item.clone());
            out["items"] = json!(admitted);
            out["nextCursor"] = if index + 1 < rows.len() || more {
                json!(Token {
                    snapshot: id,
                    kind: lease.query.kind_tag(),
                    owner: *owner,
                    position: *position,
                    utf16: 0,
                    items: u16::try_from(lease.query.items).map_err(|_| invalid())?,
                    wire: u32::try_from(lease.query.wire).map_err(|_| invalid())?,
                    binding: [0; 16]
                }
                .encode(&key)?)
            } else {
                Value::Null
            };
            if wire_len(&out, rpc_id) > lease.query.wire {
                admitted.pop();
                if admitted.is_empty() {
                    return Err(budget());
                }
                let (_, owner, position) = &rows[index - 1];
                out["items"] = json!(admitted);
                out["nextCursor"] = json!(Token {
                    snapshot: id,
                    kind: lease.query.kind_tag(),
                    owner: *owner,
                    position: *position,
                    utf16: 0,
                    items: u16::try_from(lease.query.items).map_err(|_| invalid())?,
                    wire: u32::try_from(lease.query.wire).map_err(|_| invalid())?,
                    binding: [0; 16]
                }
                .encode(&key)?);
                break;
            }
        }
        if wire_len(&out, rpc_id) > lease.query.wire {
            return Err(budget());
        }
        self.validate_annotation_lease(&lease, scope, principal)
            .await?;
        Ok(out)
    }

    async fn annotation_summary_rows(
        &self,
        lease: &Lease,
        id: Uuid,
        epochs: &AnnotationEpochs,
        cursor: Option<&Token>,
        key: &[u8],
    ) -> Result<(Value, Vec<(Value, u64, u64)>, bool)> {
        let ws = WorkspaceId::from(lease.scope.workspace_id.as_str());
        let note = NoteId::from(lease.scope.note_id.as_str());
        let ranges = lease
            .query
            .ranges
            .iter()
            .map(|r| SourceRange {
                start: r.start,
                end: r.end,
            })
            .collect::<Vec<_>>();
        let head: i64 = sqlx::query_scalar(
            "SELECT id FROM note_annotation_head WHERE workspace_id=? AND note_id=?",
        )
        .bind(ws.as_str())
        .bind(note.as_str())
        .fetch_one(self.read_pool())
        .await
        .map_err(db_error)?;
        let mut items = Vec::new();
        match lease.query.kind {
            AnnotationKind::Attribution => {
                let rows = self
                    .read_attribution_rows(
                        &ws,
                        &note,
                        epochs,
                        &ranges,
                        cursor.map(|c| integer(c.owner)).transpose()?,
                        lease.query.items,
                    )
                    .await?;
                let mut out = envelope(lease, id, "noteAttributionPage");
                out["state"] = json!(if epochs.attribution_ready {
                    "ready"
                } else {
                    "pending"
                });
                for row in rows.items {
                    let line = number(row.line)?;
                    let item = json!({"id":format!("line:{line}"),"sourceRange":{"start":row.source_range.start,"end":row.source_range.end},"startLine":line,"endLine":line,"authorRef":if row.has_author {Some(reference(id,20,line,key)?)}else{None},"timestamp":row.timestamp});
                    items.push((item, line, 0));
                }
                Ok((out, items, rows.has_more))
            }
            AnnotationKind::Comments => {
                let after = if let Some(c) = cursor {
                    Some((
                        integer(c.position)?,
                        sqlx::query_scalar::<_, String>(
                            "SELECT thread_id FROM note_comment_thread WHERE head_id=? AND rowid=?",
                        )
                        .bind(head)
                        .bind(integer(c.owner)?)
                        .fetch_optional(self.read_pool())
                        .await
                        .map_err(db_error)?
                        .ok_or_else(bad_cursor)?,
                    ))
                } else {
                    None
                };
                let filter = match lease.query.anchor_state {
                    AnchorFilter::Anchored => CommentFilter::Anchored,
                    AnchorFilter::Orphaned => CommentFilter::Orphaned,
                    AnchorFilter::All => CommentFilter::All,
                };
                let rows = if filter == CommentFilter::Anchored {
                    self.read_annotation_matches(
                        lease,
                        id,
                        epochs,
                        after.as_ref().map(|(p, t)| (*p, t.as_str())),
                    )
                    .await?
                } else {
                    let rows = self
                        .read_comment_threads(
                            &ws,
                            &note,
                            epochs,
                            &ranges,
                            filter,
                            after.as_ref().map(|(p, t)| (*p, t.as_str())),
                            lease.query.items,
                        )
                        .await?;
                    rows
                };
                let mut out = envelope(lease, id, "noteCommentPage");
                out["totalThreads"] = json!(rows.total_threads);
                out["totalComments"] = json!(rows.total_comments);
                for row in rows.page.items {
                    let owner: i64 = sqlx::query_scalar(
                        "SELECT rowid FROM note_comment_thread WHERE head_id=? AND thread_id=?",
                    )
                    .bind(head)
                    .bind(&row.thread_id)
                    .fetch_one(self.read_pool())
                    .await
                    .map_err(db_error)?;
                    let root = if row.root_present {
                        row.root_comment_id
                            .as_deref()
                            .unwrap_or(&row.latest_comment_id)
                    } else {
                        &row.latest_comment_id
                    };
                    let detail:i64=sqlx::query_scalar("SELECT rowid FROM note_comment_projection WHERE head_id=? AND comment_id=?").bind(head).bind(root).fetch_one(self.read_pool()).await.map_err(db_error)?;
                    let preview = trim(&row.latest_comment_preview, 512);
                    items.push((json!({"threadId":row.thread_id,"status":row.status,"totalComments":row.total_comments,"latestCommentId":row.latest_comment_id,"latestCommentPreview":preview,"truncated":row.truncated||preview.len()<row.latest_comment_preview.len(),"anchorRef":if row.root_present{Some(reference(id,30,number(detail)?,key)?)}else{None},"detailRef":reference(id,31,number(detail)?,key)?}),number(owner)?,number(row.position)?));
                }
                Ok((out, items, rows.page.has_more))
            }
            AnnotationKind::Replies => {
                let after = if let Some(c) = cursor {
                    let row=sqlx::query("SELECT created_at,comment_id FROM note_comment_projection WHERE head_id=? AND rowid=? AND thread_id=?").bind(head).bind(integer(c.owner)?).bind(&lease.query.thread_id).fetch_optional(self.read_pool()).await.map_err(db_error)?.ok_or_else(bad_cursor)?;
                    Some((
                        row.get::<String, _>("created_at"),
                        row.get::<String, _>("comment_id"),
                    ))
                } else {
                    None
                };
                let rows = self
                    .read_comment_rows(
                        &ws,
                        &note,
                        epochs,
                        lease.query.thread_id.as_deref().ok_or_else(invalid)?,
                        after.as_ref().map(|(a, b)| (a.as_str(), b.as_str())),
                        lease.query.items,
                    )
                    .await?;
                let mut out = envelope(lease, id, "noteReplyPage");
                out["totalComments"] = json!(rows.total_comments);
                out["rootCommentId"] = json!(rows.root_comment_id);
                out["rootState"] = json!(if rows.root_present {
                    "present"
                } else {
                    "deleted"
                });
                for row in rows.page.items {
                    let owner:i64=sqlx::query_scalar("SELECT rowid FROM note_comment_projection WHERE head_id=? AND comment_id=?").bind(head).bind(&row.id).fetch_one(self.read_pool()).await.map_err(db_error)?;
                    let owner = number(owner)?;
                    let preview = trim(&row.preview, 512);
                    let mut item = json!({"commentId":row.id,"status":row.status,"createdAt":row.created_at,"preview":preview,"truncated":row.truncated||preview.len()<row.preview.len(),"bodyRef":reference(id,10,owner,key)?,"detailRef":reference(id,31,owner,key)?});
                    let presence=sqlx::query("SELECT field,is_null FROM note_comment_detail WHERE comment_id=? AND field IN ('authorPrincipalId','provider')").bind(&row.id).fetch_all(self.read_pool()).await.map_err(db_error)?;
                    for field in presence {
                        if !field.get::<bool, _>("is_null") {
                            let (name, kind) = if field.get::<String, _>("field") == "provider" {
                                ("authorIdentityRef", 32)
                            } else {
                                ("authorPrincipalIdRef", 15)
                            };
                            item[name] = json!(reference(id, kind, owner, key)?);
                        }
                    }
                    items.push((item, owner, 0));
                }
                Ok((out, items, rows.page.has_more))
            }
        }
    }
}
