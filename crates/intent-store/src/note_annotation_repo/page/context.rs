use super::listing::{envelope, integer, number};
use super::{
    bad_cursor, budget, invalid, text_id, validate_rpc_id, wire_len, AnnotationKind, Lease, Token,
};
use crate::{
    note_annotation_repo::{db_error, AnnotationDetail, AnnotationEpochs, CommentDetailField},
    Store,
};
use intent_core::{note_page::NoteScope, NoteId, Result, WorkspaceId};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::Row;

/// Annotation context request, admitted separately from source context pages.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnnotationContextRequest {
    pub kind: String,
    pub context_ref: String,
    pub cursor: Option<String>,
    pub max_items: Option<usize>,
    pub max_wire_bytes: Option<usize>,
}

fn field(kind: u8) -> Option<CommentDetailField> {
    Some(match kind {
        10 => CommentDetailField::Body,
        11 => CommentDetailField::Author,
        12 => CommentDetailField::Anchor,
        13 => CommentDetailField::AnchorText,
        14 => CommentDetailField::Extra,
        15 => CommentDetailField::AuthorPrincipalId,
        16 => CommentDetailField::Provider,
        17 => CommentDetailField::Host,
        18 => CommentDetailField::ExternalUserId,
        21 => CommentDetailField::StartId,
        22 => CommentDetailField::EndId,
        23 => CommentDetailField::PointId,
        _ => return None,
    })
}

impl Store {
    /// Resolve only authenticated annotation references in their original lease.
    /// Service membership/visibility checks precede this read.
    ///
    /// # Errors
    /// Rejects malformed references, changed scope/epochs/budgets, expiry or a
    /// complete JSON-RPC frame that cannot fit one scalar-safe fragment.
    pub async fn read_note_annotation_context(
        &self,
        principal: &str,
        scope: &NoteScope,
        source_revision: &str,
        epoch: &str,
        page: &AnnotationContextRequest,
        rpc_id: &Value,
    ) -> Result<Value> {
        validate_rpc_id(rpc_id)?;
        let items = page.max_items.unwrap_or(128);
        let wire = page.max_wire_bytes.unwrap_or(65_536);
        if !(1..=128).contains(&items) || !(4096..=65_536).contains(&wire) {
            return Err(budget());
        }
        if page.kind != "context" || !text_id(&page.context_ref) {
            return Err(invalid());
        }
        let key = self.annotation_key().await?;
        let origin = Token::decode(&page.context_ref, &key)?;
        if origin.binding != [0; 16]
            || origin.items != 0
            || origin.wire != 0
            || origin.kind >= 128
            || !(field(origin.kind).is_some() || [20, 30, 31, 32].contains(&origin.kind))
        {
            return Err(bad_cursor());
        }
        let binding: [u8; 16] = Sha256::digest(page.context_ref.as_bytes())[..16]
            .try_into()
            .map_err(|_| invalid())?;
        let lease = self.annotation_lease(origin.snapshot).await?;
        if source_revision != lease.source_revision
            || epoch != lease.epoch
            || (lease.query.kind == AnnotationKind::Attribution) != (origin.kind == 20)
        {
            return Err(bad_cursor());
        }
        let epochs = self
            .validate_annotation_lease(&lease, scope, principal)
            .await?;
        if origin.kind == 30 && !epochs.anchors_ready {
            return Err(crate::note_annotation_repo::stale());
        }
        let mut token = if let Some(cursor) = &page.cursor {
            let cursor = Token::decode(cursor, &key)?;
            if cursor.snapshot != origin.snapshot
                || cursor.kind != origin.kind | 128
                || cursor.owner != origin.owner
                || cursor.items != u16::try_from(items).map_err(|_| invalid())?
                || cursor.wire != u32::try_from(wire).map_err(|_| invalid())?
                || cursor.binding != binding
                || cursor.position < origin.position
            {
                return Err(bad_cursor());
            }
            cursor
        } else {
            origin.clone()
        };
        token.kind = origin.kind;
        token.binding = binding;
        token.items = u16::try_from(items).map_err(|_| invalid())?;
        token.wire = u32::try_from(wire).map_err(|_| invalid())?;
        let mut out = envelope(&lease, origin.snapshot, "noteContextPage");
        let mut output = Vec::new();
        let scalar = field(token.kind).is_some() || token.kind == 20;
        if scalar {
            let mut maximum = 16_384;
            loop {
                let (item, next) = self
                    .annotation_fragment_item(&lease, &epochs, &token, maximum, &key)
                    .await?;
                out["items"] = json!([item]);
                out["nextCursor"] = if let Some(mut next) = next {
                    next.kind |= 128;
                    json!(next.encode(&key)?)
                } else {
                    Value::Null
                };
                if wire_len(&out, rpc_id) <= wire {
                    break;
                }
                if maximum <= 4 {
                    return Err(budget());
                }
                maximum = (maximum / 2).max(4);
            }
        } else {
            let kinds: &[u8] = match token.kind {
                30 => &[12, 13],
                31 => &[10, 11, 12, 13, 14],
                32 => &[16, 17, 18],
                _ => return Err(bad_cursor()),
            };
            let mut index = usize::try_from(token.position).map_err(|_| bad_cursor())?;
            while output.len() < items && index < kinds.len() {
                let mut scalar_token = token.clone();
                scalar_token.kind = kinds[index];
                scalar_token.position = 0;
                scalar_token.utf16 = 0;
                let (_, comment_id) = self.annotation_comment_owner(&lease, token.owner).await?;
                let present: bool = sqlx::query_scalar(
                    "SELECT NOT is_null FROM note_comment_detail WHERE comment_id=? AND field=?",
                )
                .bind(&comment_id)
                .bind(field(scalar_token.kind).ok_or_else(invalid)?.name())
                .fetch_one(self.read_pool())
                .await
                .map_err(db_error)?;
                if !present {
                    index += 1;
                    token.position = u64::try_from(index).map_err(|_| invalid())?;
                    continue;
                }
                let (item, _) = self
                    .annotation_fragment_item(&lease, &epochs, &scalar_token, 128, &key)
                    .await?;
                let previous = out.clone();
                output.push(item);
                let mut next = token.clone();
                next.position = number(i64::try_from(index + 1).map_err(|_| invalid())?)?;
                next.kind |= 128;
                out["items"] = json!(output);
                out["nextCursor"] = if index + 1 < kinds.len() || token.kind == 30 {
                    json!(next.encode(&key)?)
                } else {
                    Value::Null
                };
                if wire_len(&out, rpc_id) > wire {
                    output.pop();
                    if output.is_empty() {
                        return Err(budget());
                    }
                    out = previous;
                    break;
                }
                index += 1;
                token.position = u64::try_from(index).map_err(|_| invalid())?;
            }
            if token.kind == 30 && index >= kinds.len() && output.len() < items {
                let (head, comment_id) = self.annotation_comment_owner(&lease, token.owner).await?;
                let canonical=sqlx::query("SELECT d.field,d.byte_length,p.data FROM note_comment_detail d LEFT JOIN note_comment_detail_piece p ON p.comment_id=d.comment_id AND p.field=d.field AND p.position=0 WHERE d.comment_id=? AND d.field IN ('startId','pointId') AND d.is_null=0 ORDER BY d.field LIMIT 1").bind(&comment_id).fetch_optional(self.read_pool()).await.map_err(db_error)?;
                let canonical = canonical
                    .map(|row| {
                        if row.get::<i64, _>("byte_length") > 256 {
                            return Err(budget());
                        }
                        String::from_utf8(row.get::<Option<Vec<u8>>, _>("data").unwrap_or_default())
                            .map_err(|_| invalid())
                    })
                    .transpose()?;
                let after = if token.position > 2 {
                    integer(token.position - 2)?
                } else {
                    0
                };
                let rows=sqlx::query("SELECT id,occurrence_id,start,end FROM note_comment_anchor WHERE head_id=? AND comment_id=? AND id>? ORDER BY id LIMIT ?")
                    .bind(head).bind(&comment_id).bind(after).bind(i64::try_from(items-output.len()+1).map_err(|_|invalid())?).fetch_all(self.read_pool()).await.map_err(db_error)?;
                for (offset, row) in rows.iter().enumerate().take(items - output.len()) {
                    let occurrence: i64 = row.get("id");
                    let previous = out.clone();
                    output.push(json!({"kind":"span","id":format!("{}:{occurrence}",token.snapshot.simple()),"occurrenceId":format!("{}:{occurrence}",token.snapshot.simple()),"canonicalId":canonical.as_deref().ok_or_else(invalid)?,"sourceRange":{"start":row.get::<i64,_>("start"),"end":row.get::<i64,_>("end")},"role":"commentAnchor"}));
                    let mut next = token.clone();
                    next.position = number(occurrence)? + 2;
                    next.kind |= 128;
                    out["items"] = json!(output);
                    out["nextCursor"] = if offset + 1 < rows.len() {
                        json!(next.encode(&key)?)
                    } else {
                        Value::Null
                    };
                    if wire_len(&out, rpc_id) > wire {
                        output.pop();
                        if output.is_empty() {
                            return Err(budget());
                        }
                        out = previous;
                        break;
                    }
                    token.position = next.position;
                }
                if rows.is_empty() {
                    out["nextCursor"] = Value::Null;
                    out["orphaned"] = json!(after == 0);
                }
            } else {
                token.position = u64::try_from(index).map_err(|_| invalid())?;
            }
            if token.kind != 30 && index >= kinds.len() {
                out["nextCursor"] = Value::Null;
            }
            out["items"] = json!(output);
            if output.is_empty() && out["nextCursor"] != Value::Null {
                return Err(budget());
            }
            // Rebuild continuation from the last admitted item after a size cut.
            if out["nextCursor"] != Value::Null {
                token.kind |= 128;
                out["nextCursor"] = json!(token.encode(&key)?);
            }
        }
        if wire_len(&out, rpc_id) > wire {
            return Err(budget());
        }
        self.validate_annotation_lease(&lease, scope, principal)
            .await?;
        Ok(out)
    }

    async fn annotation_comment_owner(&self, lease: &Lease, owner: u64) -> Result<(i64, String)> {
        let row=sqlx::query("SELECT p.head_id,p.comment_id FROM note_comment_projection p JOIN note_annotation_head h ON h.id=p.head_id WHERE h.workspace_id=? AND h.note_id=? AND p.rowid=?")
            .bind(&lease.scope.workspace_id).bind(&lease.scope.note_id).bind(integer(owner)?).fetch_optional(self.read_pool()).await.map_err(db_error)?.ok_or_else(bad_cursor)?;
        Ok((row.get("head_id"), row.get("comment_id")))
    }

    async fn annotation_fragment_item(
        &self,
        lease: &Lease,
        epochs: &AnnotationEpochs,
        token: &Token,
        maximum: usize,
        key: &[u8],
    ) -> Result<(Value, Option<Token>)> {
        let ws = WorkspaceId::from(lease.scope.workspace_id.as_str());
        let note = NoteId::from(lease.scope.note_id.as_str());
        let comment_id = if token.kind == 20 {
            String::new()
        } else {
            self.annotation_comment_owner(lease, token.owner).await?.1
        };
        let (target, name) = if token.kind == 20 {
            (
                AnnotationDetail::AttributionAuthor {
                    line: integer(token.owner)?,
                },
                "author",
            )
        } else {
            let field = field(token.kind).ok_or_else(bad_cursor)?;
            (
                AnnotationDetail::Comment {
                    comment_id: &comment_id,
                    field,
                },
                field.name(),
            )
        };
        let fragment = self
            .read_annotation_fragment(
                &ws,
                &note,
                epochs,
                target,
                integer(token.position)?,
                maximum,
            )
            .await?;
        if fragment.is_null {
            return Err(invalid());
        }
        let next = if fragment.byte_end < fragment.total_bytes {
            let mut next = token.clone();
            next.position = number(fragment.byte_end)?;
            next.utf16 += u64::try_from(fragment.utf16_length).map_err(|_| invalid())?;
            Some(next)
        } else {
            None
        };
        let item = json!({"kind":"fragment","id":if token.kind==20 {format!("line:{}",token.owner)}else{comment_id},"field":name,"offset":token.utf16,"text":fragment.text,"nextRef":next.as_ref().map(|t|{let mut reference=t.clone();reference.items=0;reference.wire=0;reference.binding=[0;16];reference.encode(key)}).transpose()?});
        Ok((item, next))
    }
}
