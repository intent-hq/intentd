use super::{
    db_error, head, invalid, stale, validate_limit, validate_ranges, AnnotationEpochs,
    AnnotationPage, SourceRange,
};
use crate::Store;
use intent_core::{Error, NoteId, Result, WorkspaceId};
use sqlx::QueryBuilder;
use sqlx::{Row, SqliteConnection};

/// A resolved occurrence of an existing canonical root comment anchor. The
/// occurrence ID is local to this projection, never a replacement marker ID.
#[derive(Clone, Debug)]
pub struct AnchorOccurrence {
    pub comment_id: String,
    pub occurrence_id: String,
    pub source_range: SourceRange,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommentFilter {
    Anchored,
    Orphaned,
    All,
}

#[derive(Clone, Debug)]
pub struct CommentRow {
    pub id: String,
    pub status: String,
    pub created_at: String,
    pub preview: String,
    pub truncated: bool,
}

#[derive(Clone, Debug)]
pub struct ThreadRow {
    pub thread_id: String,
    pub status: String,
    pub total_comments: i64,
    pub root_comment_id: Option<String>,
    pub latest_comment_id: String,
    pub latest_comment_preview: String,
    pub truncated: bool,
    pub position: i64,
}

#[derive(Clone, Debug)]
pub struct ReplyRows {
    pub page: AnnotationPage<CommentRow>,
    pub total_comments: i64,
    pub root_comment_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct ThreadRows {
    pub page: AnnotationPage<ThreadRow>,
    pub total_threads: i64,
    pub total_comments: i64,
}

fn check_epoch(actual: &AnnotationEpochs, expected: &AnnotationEpochs) -> Result<()> {
    if actual.source_revision != expected.source_revision
        || actual.comment_revision != expected.comment_revision
    {
        return Err(stale());
    }
    Ok(())
}

pub(super) fn matching_threads<'args>(
    head_id: i64,
    ranges: &[SourceRange],
    filter: CommentFilter,
) -> QueryBuilder<'args, sqlx::Sqlite> {
    let mut query = QueryBuilder::new("WITH matched AS (");
    if filter == CommentFilter::Anchored && !ranges.is_empty() {
        query.push("SELECT thread_id,MIN(position) AS position FROM (");
        for (index, range) in ranges.iter().enumerate() {
            if index > 0 {
                query.push(" UNION ALL ");
            }
            query
                .push("SELECT p.thread_id,MAX(a.start,")
                .push_bind(range.start)
                .push(
                    ") AS position \
                FROM note_comment_anchor_extent r CROSS JOIN note_comment_anchor a ON a.id=r.id \
                JOIN note_comment_projection p ON p.comment_id=a.comment_id \
                WHERE r.scope_min<=",
                )
                .push_bind(head_id)
                .push(" AND r.scope_max>=")
                .push_bind(head_id)
                .push(" AND r.start<=")
                .push_bind(range.end)
                .push(" AND r.end>=")
                .push_bind(range.start)
                .push(" AND a.head_id=")
                .push_bind(head_id)
                .push(" AND a.start<")
                .push_bind(range.end)
                .push(" AND (a.end>")
                .push_bind(range.start)
                .push(" OR (a.start=a.end AND a.start>=")
                .push_bind(range.start)
                .push("))");
        }
        query.push(") GROUP BY thread_id");
    } else {
        query
            .push("SELECT t.thread_id,0 AS position FROM note_comment_thread t WHERE t.head_id=")
            .push_bind(head_id);
        match filter {
            CommentFilter::Anchored => {
                query.push(" AND 0");
            }
            CommentFilter::All => {}
            CommentFilter::Orphaned => {
                query.push(" AND NOT EXISTS (SELECT 1 FROM note_comment_projection p JOIN note_comment_anchor a ON a.comment_id=p.comment_id \
                    WHERE p.head_id=t.head_id AND p.thread_id=t.thread_id AND a.head_id=t.head_id)");
            }
        }
    }
    query.push(") ");
    query
}

/// Replace a derived anchor index inside the caller's source/comment transaction.
/// The caller must roll its transaction back on error and must have completed
/// canonical marker repair plus source page indexing before calling this.
pub(crate) async fn publish_anchors_in_transaction(
    conn: &mut SqliteConnection,
    workspace_id: &WorkspaceId,
    note_id: &NoteId,
    expected: &AnnotationEpochs,
    occurrences: &[AnchorOccurrence],
) -> Result<()> {
    let (id, epochs) = head(conn, workspace_id, note_id).await?;
    check_epoch(&epochs, expected)?;
    if epochs.anchors_ready {
        return Err(stale());
    }
    let source_length: i64 = sqlx::query_scalar("SELECT source_length FROM note_page_head WHERE workspace_id=? AND note_id=? AND indexed_rev=current_rev")
        .bind(workspace_id.as_str()).bind(note_id.as_str()).fetch_optional(&mut *conn).await.map_err(db_error)?
        .ok_or_else(stale)?;
    sqlx::query("DELETE FROM note_comment_anchor WHERE head_id=?")
        .bind(id)
        .execute(&mut *conn)
        .await
        .map_err(db_error)?;
    for occurrence in occurrences {
        let range = occurrence.source_range;
        if range.start < 0
            || range.end < range.start
            || range.end > source_length
            || occurrence.occurrence_id.is_empty()
        {
            return Err(invalid());
        }
        let result = sqlx::query("INSERT INTO note_comment_anchor(head_id,comment_id,occurrence_id,start,end) \
            SELECT head_id,comment_id,?,?,? FROM note_comment_projection WHERE head_id=? AND comment_id=? AND parent_id IS NULL")
            .bind(&occurrence.occurrence_id).bind(range.start).bind(range.end).bind(id).bind(&occurrence.comment_id)
            .execute(&mut *conn).await.map_err(db_error)?;
        if result.rows_affected() != 1 {
            return Err(invalid());
        }
    }
    sqlx::query("UPDATE note_annotation_head SET anchors_rev=source_rev WHERE id=?")
        .bind(id)
        .execute(&mut *conn)
        .await
        .map_err(db_error)?;
    Ok(())
}

impl Store {
    /// Publish resolved marker occurrences after canonical anchor repair.
    /// Caller supplies the source/comment epochs it resolved. A concurrent edit
    /// or comment mutation rejects the entire derived replacement.
    ///
    /// # Errors
    /// Returns invalid params for invalid/rootless occurrences, stale for a
    /// superseded projection, or a database error; no partial index is visible.
    pub async fn publish_comment_anchors(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
        expected: &AnnotationEpochs,
        occurrences: &[AnchorOccurrence],
    ) -> Result<()> {
        let mut tx = self
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(db_error)?;
        super::publish_anchors_in_transaction(
            &mut tx,
            workspace_id,
            note_id,
            expected,
            occurrences,
        )
        .await?;
        tx.commit().await.map_err(db_error)
    }

    /// Keyset page of root/reply summaries in a single scoped thread. The root
    /// follows the same bound and order as replies, without a full-body exception.
    ///
    /// # Errors
    /// Returns invalid params for bad limits, stale for old epochs, not found
    /// for an absent scoped thread, or a database error.
    pub async fn read_comment_rows(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
        expected: &AnnotationEpochs,
        thread_id: &str,
        after: Option<(&str, &str)>,
        limit: usize,
    ) -> Result<ReplyRows> {
        let take = validate_limit(limit)?;
        let mut tx = self.read_pool().begin().await.map_err(db_error)?;
        let (id, epochs) = head(&mut tx, workspace_id, note_id).await?;
        check_epoch(&epochs, expected)?;
        let total_comments = sqlx::query_scalar(
            "SELECT total_comments FROM note_comment_thread WHERE head_id=? AND thread_id=?",
        )
        .bind(id)
        .bind(thread_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db_error)?
        .ok_or_else(|| Error::NotFound("comment thread".into()))?;
        let root_comment_id = sqlx::query_scalar("SELECT comment_id FROM note_comment_projection WHERE head_id=? AND thread_id=? AND parent_id IS NULL ORDER BY created_at,comment_id LIMIT 1")
            .bind(id).bind(thread_id).fetch_optional(&mut *tx).await.map_err(db_error)?;
        let mut query = QueryBuilder::new("SELECT comment_id,status,created_at,preview,truncated FROM note_comment_projection WHERE head_id=");
        query
            .push_bind(id)
            .push(" AND thread_id=")
            .push_bind(thread_id);
        if let Some((created_at, comment_id)) = after {
            query
                .push(" AND (created_at,comment_id)>(")
                .push_bind(created_at)
                .push(",")
                .push_bind(comment_id)
                .push(")");
        }
        query
            .push(" ORDER BY created_at,comment_id LIMIT ")
            .push_bind(take);
        let rows = query.build().fetch_all(&mut *tx).await.map_err(db_error)?;
        let has_more = rows.len() > limit;
        let items = rows
            .iter()
            .take(limit)
            .map(|row| CommentRow {
                id: row.get("comment_id"),
                status: row.get("status"),
                created_at: row.get("created_at"),
                preview: row.get("preview"),
                truncated: row.get("truncated"),
            })
            .collect();
        tx.commit().await.map_err(db_error)?;
        Ok(ReplyRows {
            page: AnnotationPage {
                epochs,
                items,
                has_more,
            },
            total_comments,
            root_comment_id,
        })
    }

    /// Page visible threads or the explicit all/orphaned sidebar. Totals use
    /// only indexed occurrences and maintained integer counts, never bodies.
    ///
    /// # Errors
    /// Returns invalid params for invalid filters/ranges/limits, stale for
    /// mismatched or unresolved source anchors, or a database error.
    #[expect(clippy::too_many_arguments)] // Scoped range/keyset/budget inputs stay explicit.
    pub async fn read_comment_threads(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
        expected: &AnnotationEpochs,
        ranges: &[SourceRange],
        filter: CommentFilter,
        after: Option<(i64, &str)>,
        limit: usize,
    ) -> Result<ThreadRows> {
        validate_ranges(ranges)?;
        let take = validate_limit(limit)?;
        if filter != CommentFilter::Anchored && !ranges.is_empty() {
            return Err(invalid());
        }
        let mut tx = self.read_pool().begin().await.map_err(db_error)?;
        let (id, epochs) = head(&mut tx, workspace_id, note_id).await?;
        check_epoch(&epochs, expected)?;
        if !epochs.anchors_ready && filter != CommentFilter::All {
            return Err(stale());
        }
        let mut counts = matching_threads(id, ranges, filter);
        counts.push("SELECT COUNT(*) AS threads,COALESCE(SUM(t.total_comments),0) AS comments FROM matched m JOIN note_comment_thread t ON t.thread_id=m.thread_id WHERE t.head_id=").push_bind(id);
        let totals = counts.build().fetch_one(&mut *tx).await.map_err(db_error)?;
        let mut query = matching_threads(id, ranges, filter);
        query.push(", selected AS MATERIALIZED (SELECT thread_id,position FROM matched WHERE 1");
        if let Some((position, thread_id)) = after {
            query
                .push(" AND (position,thread_id)>(")
                .push_bind(position)
                .push(",")
                .push_bind(thread_id)
                .push(")");
        }
        query
            .push(" ORDER BY position,thread_id LIMIT ")
            .push_bind(take)
            .push(") ");
        query.push("SELECT t.thread_id,t.total_comments,m.position,root.comment_id AS root_id,COALESCE(root.status,latest.status) AS status,latest.comment_id AS latest_id,latest.preview,latest.truncated \
            FROM selected m JOIN note_comment_thread t ON t.thread_id=m.thread_id \
            LEFT JOIN note_comment_projection root ON root.comment_id=(SELECT comment_id FROM note_comment_projection WHERE head_id=t.head_id AND thread_id=t.thread_id AND parent_id IS NULL ORDER BY created_at,comment_id LIMIT 1) \
            JOIN note_comment_projection latest ON latest.comment_id=(SELECT comment_id FROM note_comment_projection WHERE head_id=t.head_id AND thread_id=t.thread_id ORDER BY created_at DESC,comment_id DESC LIMIT 1) \
            WHERE t.head_id=").push_bind(id);
        query.push(" ORDER BY m.position,t.thread_id");
        let rows = query.build().fetch_all(&mut *tx).await.map_err(db_error)?;
        let has_more = rows.len() > limit;
        let items = rows
            .iter()
            .take(limit)
            .map(|row| ThreadRow {
                thread_id: row.get("thread_id"),
                total_comments: row.get("total_comments"),
                position: row.get("position"),
                status: row.get("status"),
                root_comment_id: row.get("root_id"),
                latest_comment_id: row.get("latest_id"),
                latest_comment_preview: row.get("preview"),
                truncated: row.get("truncated"),
            })
            .collect();
        tx.commit().await.map_err(db_error)?;
        Ok(ThreadRows {
            page: AnnotationPage {
                epochs,
                items,
                has_more,
            },
            total_threads: totals.get("threads"),
            total_comments: totals.get("comments"),
        })
    }
}
