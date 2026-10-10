//! Epoch-bound annotation page admission, leases, and exact frame budgeting.
use super::{db_error, invalid, stale, validate_ranges, AnnotationEpochs, SourceRange, MAX_OFFSET};
use crate::Store;
use intent_core::{note_page::NotePageError, Error, NoteId, Result, WorkspaceId};
use sqlx::Row;
mod context;
mod listing;
pub(super) mod matches;
use super::token::Token;
pub use context::AnnotationContextRequest;
use intent_core::note_page::NoteScope;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AnnotationKind {
    Attribution,
    Comments,
    Replies,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AnchorFilter {
    #[default]
    Anchored,
    Orphaned,
    All,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnnotationRange {
    pub start: i64,
    pub end: i64,
}

/// Strict page object for existing opt-in annotation methods. Scope and epochs
/// belong to the enclosing request and are supplied separately by the service.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AnnotationPageRequest {
    pub kind: AnnotationKind,
    pub ranges: Option<Vec<AnnotationRange>>,
    pub anchor_state: Option<AnchorFilter>,
    pub cursor: Option<String>,
    pub max_items: Option<usize>,
    pub max_wire_bytes: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Query {
    kind: AnnotationKind,
    ranges: Vec<AnnotationRange>,
    anchor_state: AnchorFilter,
    thread_id: Option<String>,
    items: usize,
    wire: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Lease {
    scope: NoteScope,
    principal: String,
    source_revision: String,
    epoch: String,
    query: Query,
    expires_at: String,
    expires_ms: i64,
}

fn failure(kind: NotePageError) -> Error {
    Error::NotePage(kind)
}
fn budget() -> Error {
    failure(NotePageError::Budget)
}
fn bad_cursor() -> Error {
    failure(NotePageError::CursorInvalid)
}
fn text_id(text: &str) -> bool {
    !text.is_empty() && text.len() <= 256
}
fn wire_len(result: &Value, rpc_id: &Value) -> usize {
    json!({"jsonrpc":"2.0","id":rpc_id,"result":result})
        .to_string()
        .len()
}
fn validate_rpc_id(id: &Value) -> Result<()> {
    if id.as_str().is_some_and(|s| s.len() <= 64)
        || id
            .as_i64()
            .is_some_and(|n| (-MAX_OFFSET..=MAX_OFFSET).contains(&n))
        || id
            .as_u64()
            .is_some_and(|n| n <= u64::try_from(MAX_OFFSET).expect("positive bound"))
    {
        Ok(())
    } else {
        Err(invalid())
    }
}

impl Query {
    fn admit(page: &AnnotationPageRequest, thread_id: Option<&str>) -> Result<Self> {
        let items = page.max_items.unwrap_or(64);
        let wire = page.max_wire_bytes.unwrap_or(65_536);
        if !(1..=64).contains(&items) || !(4096..=65_536).contains(&wire) {
            return Err(budget());
        }
        if page.cursor.as_deref().is_some_and(|c| !text_id(c)) {
            return Err(bad_cursor());
        }
        let ranges = page.ranges.clone().unwrap_or_default();
        validate_ranges(
            &ranges
                .iter()
                .map(|r| SourceRange {
                    start: r.start,
                    end: r.end,
                })
                .collect::<Vec<_>>(),
        )?;
        let anchor_state = page.anchor_state.unwrap_or_default();
        match page.kind {
            AnnotationKind::Attribution => {
                if page.ranges.is_none() || page.anchor_state.is_some() || thread_id.is_some() {
                    return Err(invalid());
                }
            }
            AnnotationKind::Comments => {
                if page.ranges.is_none()
                    || thread_id.is_some()
                    || (anchor_state != AnchorFilter::Anchored && !ranges.is_empty())
                {
                    return Err(invalid());
                }
            }
            AnnotationKind::Replies => {
                if page.ranges.is_some()
                    || page.anchor_state.is_some()
                    || !thread_id.is_some_and(text_id)
                {
                    return Err(invalid());
                }
            }
        }
        Ok(Self {
            kind: page.kind,
            ranges,
            anchor_state,
            thread_id: thread_id.map(str::to_owned),
            items,
            wire,
        })
    }
    fn kind_tag(&self) -> u8 {
        match self.kind {
            AnnotationKind::Attribution => 1,
            AnnotationKind::Comments => 2,
            AnnotationKind::Replies => 3,
        }
    }
}

impl Store {
    async fn annotation_key(&self) -> Result<Vec<u8>> {
        sqlx::query_scalar("SELECT token_key FROM note_page_backend WHERE singleton=1")
            .fetch_one(self.read_pool())
            .await
            .map_err(db_error)
    }
    async fn save_annotation_lease(&self, lease: &Lease) -> Result<Uuid> {
        let id = Uuid::new_v4();
        let now = i64::try_from(intent_core::now_epoch_ms()).map_err(|_| invalid())?;
        let payload = serde_json::to_string(lease).map_err(|_| invalid())?;
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_error)?;
        sqlx::query("DELETE FROM note_annotation_snapshot WHERE expires_ms<=?")
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_annotation_snapshot")
            .fetch_one(&mut *tx)
            .await
            .map_err(db_error)?;
        if count >= 256 {
            sqlx::query("DELETE FROM note_annotation_snapshot WHERE id=(SELECT id FROM note_annotation_snapshot ORDER BY expires_ms,id LIMIT 1)")
                .execute(&mut *tx).await.map_err(db_error)?;
        }
        sqlx::query("INSERT INTO note_annotation_snapshot(id,expires_ms,payload) VALUES(?,?,?)")
            .bind(id.simple().to_string())
            .bind(lease.expires_ms)
            .bind(payload)
            .execute(&mut *tx)
            .await
            .map_err(db_error)?;
        tx.commit().await.map_err(db_error)?;
        Ok(id)
    }
    async fn retire_annotation_lease(&self, id: Uuid) -> Result<()> {
        sqlx::query("DELETE FROM note_annotation_snapshot WHERE id=?")
            .bind(id.simple().to_string())
            .execute(self.write_pool())
            .await
            .map_err(db_error)?;
        Ok(())
    }
    async fn annotation_lease(&self, id: Uuid) -> Result<Lease> {
        let row = sqlx::query("SELECT expires_ms,payload FROM note_annotation_snapshot WHERE id=?")
            .bind(id.simple().to_string())
            .fetch_optional(self.read_pool())
            .await
            .map_err(db_error)?
            .ok_or_else(|| failure(NotePageError::Expired))?;
        let lease: Lease = serde_json::from_str(&row.get::<String, _>("payload"))
            .map_err(|_| Error::Internal("invalid annotation snapshot".into()))?;
        if row.get::<i64, _>("expires_ms") != lease.expires_ms
            || lease.expires_ms
                <= i64::try_from(intent_core::now_epoch_ms()).map_err(|_| invalid())?
        {
            self.retire_annotation_lease(id).await?;
            return Err(failure(NotePageError::Expired));
        }
        Ok(lease)
    }
    async fn validate_annotation_lease(
        &self,
        id: Uuid,
        lease: &Lease,
        scope: &NoteScope,
        principal: &str,
    ) -> Result<AnnotationEpochs> {
        if &lease.scope != scope || lease.principal != principal {
            return Err(bad_cursor());
        }
        let retained = self.annotation_lease(id).await?;
        if &retained != lease {
            self.retire_annotation_lease(id).await?;
            return Err(failure(NotePageError::Expired));
        }
        let ws = WorkspaceId::from(scope.workspace_id.as_str());
        let note = NoteId::from(scope.note_id.as_str());
        let state = self
            .read_note_page_state(&ws, &note, Some(&scope.note_instance_id))
            .await?;
        let epoch_field = if lease.query.kind == AnnotationKind::Attribution {
            "attributionGeneration"
        } else {
            "commentRevision"
        };
        if state["deleted"] == true
            || state["sourceRevision"] != lease.source_revision
            || state[epoch_field] != lease.epoch
        {
            return Err(stale());
        }
        if state["scope"] != serde_json::to_value(scope).map_err(|_| invalid())? {
            return Err(bad_cursor());
        }
        let epochs = self.note_annotation_epochs(&ws, &note).await?;
        let actual = if lease.query.kind == AnnotationKind::Attribution {
            &epochs.attribution_generation
        } else {
            &epochs.comment_revision
        };
        if actual != &lease.epoch {
            return Err(stale());
        }
        if self.annotation_lease(id).await? != *lease {
            self.retire_annotation_lease(id).await?;
            return Err(failure(NotePageError::Expired));
        }
        Ok(epochs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn annotation_admission_preserves_disjoint_queries_and_rejects_wide_shapes() {
        let valid = serde_json::json!({"kind":"comments","ranges":[{"start":1,"end":3},{"start":8,"end":10}],"maxItems":64,"maxWireBytes":4096});
        let page: AnnotationPageRequest = serde_json::from_value(valid.clone()).unwrap();
        let admitted = Query::admit(&page, None).unwrap();
        assert_eq!(admitted.ranges.len(), 2);
        for (name, value) in [
            ("maxItems", serde_json::json!(65)),
            ("maxWireBytes", serde_json::json!(4095)),
            (
                "ranges",
                serde_json::json!([{"start":1,"end":3},{"start":3,"end":4}]),
            ),
            ("ranges", serde_json::json!([{"start":3,"end":3}])),
            ("anchorState", serde_json::json!("all")),
        ] {
            let mut wrong = valid.clone();
            wrong[name] = value;
            let page: AnnotationPageRequest = serde_json::from_value(wrong).unwrap();
            assert!(Query::admit(&page, None).is_err());
        }
        assert!(serde_json::from_value::<AnnotationPageRequest>(
            serde_json::json!({"kind":"comments","ranges":[],"includeComments":true})
        )
        .is_err());
        assert!(validate_rpc_id(&serde_json::json!("x".repeat(65))).is_err());
        assert!(validate_rpc_id(&serde_json::json!(null)).is_err());
    }
}
