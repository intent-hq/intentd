//! Storage primitives for bounded annotations. Service/transport opt-in is
//! deliberately separate: these methods never advertise a paging capability.
//!
//! Coordinates are half-open UTF-16 offsets into one source revision. Legacy
//! comment markers remain authoritative; resolved occurrences are derived data.

use intent_core::{note_page::NotePageError, Error, NoteId, Result, WorkspaceId};
use sqlx::{Row, SqliteConnection};

use crate::Store;

mod attribution;
mod comments;

pub use attribution::{AttributionJob, AttributionRow};
pub use comments::{AnchorOccurrence, CommentFilter, CommentRow, ReplyRows, ThreadRow, ThreadRows};

/// One admitted interval in canonical source coordinates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceRange {
    pub start: i64,
    pub end: i64,
}

/// Independent persisted epochs. A source mutation invalidates both projections.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnnotationEpochs {
    pub source_revision: i64,
    pub attribution_generation: String,
    pub comment_revision: String,
    pub attribution_ready: bool,
    pub anchors_ready: bool,
}

/// A keyset page of projected rows, with no hidden full-collection hydration.
#[derive(Clone, Debug)]
pub struct AnnotationPage<T> {
    pub epochs: AnnotationEpochs,
    pub items: Vec<T>,
    pub has_more: bool,
}

const MAX_ITEMS: usize = 128;
const MAX_RANGES: usize = 32;
const MAX_OFFSET: i64 = 9_007_199_254_740_991;

fn invalid() -> Error {
    Error::InvalidParams("Invalid annotation query".into())
}

fn stale() -> Error {
    Error::NotePage(NotePageError::Stale)
}

#[expect(clippy::needless_pass_by_value)]
fn db_error(error: sqlx::Error) -> Error {
    Error::Internal(format!("annotation query: {error}"))
}

fn validate_ranges(ranges: &[SourceRange]) -> Result<()> {
    if ranges.len() > MAX_RANGES
        || ranges
            .iter()
            .any(|r| r.start < 0 || r.start >= r.end || r.end > MAX_OFFSET)
        || ranges.windows(2).any(|pair| pair[0].end >= pair[1].start)
    {
        return Err(invalid());
    }
    Ok(())
}

fn validate_limit(limit: usize) -> Result<i64> {
    if limit == 0 || limit > MAX_ITEMS {
        return Err(invalid());
    }
    i64::try_from(limit + 1).map_err(|_| invalid())
}

async fn head(
    conn: &mut SqliteConnection,
    workspace_id: &WorkspaceId,
    note_id: &NoteId,
) -> Result<(i64, AnnotationEpochs)> {
    let row = sqlx::query(
        "SELECT id, source_rev, attribution_generation, comment_revision, \
         attribution_rev = source_rev AS attribution_ready, \
         anchors_rev = source_rev AS anchors_ready FROM note_annotation_head \
         WHERE workspace_id = ? AND note_id = ?",
    )
    .bind(workspace_id.as_str())
    .bind(note_id.as_str())
    .fetch_optional(conn)
    .await
    .map_err(db_error)?
    .ok_or_else(|| Error::NotFound("note annotations".into()))?;
    Ok((
        row.get("id"),
        AnnotationEpochs {
            source_revision: row.get("source_rev"),
            attribution_generation: row.get("attribution_generation"),
            comment_revision: row.get("comment_revision"),
            attribution_ready: row.get("attribution_ready"),
            anchors_ready: row.get("anchors_ready"),
        },
    ))
}

impl Store {
    /// Read the bounded shared subscription tuple. Supplying an incarnation
    /// permits reading its deletion tombstone after a same-ID note is recreated.
    /// Omitting it selects only the live source index incarnation.
    ///
    /// # Errors
    /// Returns `NotFound` when the scoped incarnation does not exist, or a
    /// database error. Transport applies its complete-frame wire budget.
    pub async fn read_note_page_state(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
        instance_id: Option<&str>,
    ) -> Result<serde_json::Value> {
        let row = sqlx::query("SELECT s.*,b.backend_id FROM note_annotation_state s \
            CROSS JOIN note_page_backend b WHERE b.singleton=1 AND s.workspace_id=? AND s.note_id=? \
            AND s.instance_id=COALESCE(?,(SELECT instance_id FROM note_page_head WHERE workspace_id=s.workspace_id AND note_id=s.note_id))")
            .bind(workspace_id.as_str()).bind(note_id.as_str()).bind(instance_id)
            .fetch_optional(self.read_pool()).await.map_err(db_error)?
            .ok_or_else(|| Error::NotFound("note page state".into()))?;
        Ok(serde_json::json!({
            "kind": "notePageState",
            "scope": {
                "backendId": row.get::<String,_>("backend_id"),
                "workspaceId": workspace_id.as_str(), "noteId": note_id.as_str(),
                "noteInstanceId": row.get::<String,_>("instance_id"),
            },
            "stateGeneration": row.get::<String,_>("state_generation"),
            "sourceRevision": row.get::<String,_>("source_revision"),
            "attributionGeneration": row.get::<String,_>("attribution_generation"),
            "attributionState": if row.get::<bool,_>("attribution_ready") { "ready" } else { "pending" },
            "commentRevision": row.get::<String,_>("comment_revision"),
            "deleted": row.get::<bool,_>("deleted"), "invalidation": "all",
        }))
    }

    /// Read only annotation identity, without source or annotation payloads.
    ///
    /// # Errors
    /// Returns `NotFound` for a missing scoped note, or a database error.
    pub async fn note_annotation_epochs(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
    ) -> Result<AnnotationEpochs> {
        let mut conn = self.read_pool().acquire().await.map_err(db_error)?;
        Ok(head(&mut conn, workspace_id, note_id).await?.1)
    }
}

#[cfg(test)]
mod tests;
