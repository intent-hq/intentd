//! Exact initial totals scan relevant index entries and retain scalar totals.
//! Continuation seeks source-owned indexes; no matched-ID set is retained.
//! Initial aggregate cost and temporary SQL work remain O(matches), not O(page).
use super::{budget, failure, invalid, Lease};
use crate::{
    note_annotation_repo::{
        comments, db_error, head, stale, AnnotationEpochs, AnnotationPage, CommentFilter,
        SourceRange, ThreadRow, ThreadRows,
    },
    Store,
};
use intent_core::{note_page::NotePageError, NoteId, Result, WorkspaceId};
use sqlx::{Connection, QueryBuilder, Row};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use uuid::Uuid;

static PREPARATION_SLOT: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
struct CancelPreparation(Arc<AtomicBool>);
impl Drop for CancelPreparation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

async fn install_preparation_budget(
    conn: &mut sqlx::SqliteConnection,
    cancelled: Arc<AtomicBool>,
    work: Arc<AtomicUsize>,
    expires_ms: i64,
) -> Result<()> {
    let counter = work;
    conn.lock_handle()
        .await
        .map_err(db_error)?
        .set_progress_handler(1000, move || {
            counter.fetch_add(1000, Ordering::Relaxed);
            !cancelled.load(Ordering::Relaxed)
                && i64::try_from(intent_core::now_epoch_ms()).is_ok_and(|now| now < expires_ms)
        });
    Ok(())
}

impl Store {
    pub(super) async fn prepare_annotation_matches(
        &self,
        lease: &Lease,
        id: Uuid,
        expected: &AnnotationEpochs,
    ) -> Result<()> {
        self.prepare_annotation_matches_observed(lease, id, expected, Arc::new(AtomicUsize::new(0)))
            .await
    }

    async fn prepare_annotation_matches_observed(
        &self,
        lease: &Lease,
        id: Uuid,
        expected: &AnnotationEpochs,
        work: Arc<AtomicUsize>,
    ) -> Result<()> {
        let snapshot = id.simple().to_string();
        let present: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM note_annotation_match_head WHERE snapshot_id=?)",
        )
        .bind(&snapshot)
        .fetch_one(self.read_pool())
        .await
        .map_err(db_error)?;
        if present {
            return Ok(());
        }
        let admission = PREPARATION_SLOT.try_acquire().map_err(|_| budget())?;
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = CancelPreparation(Arc::clone(&cancelled));
        let store = self.clone();
        let lease = lease.clone();
        let expected = expected.clone();
        // The worker owns admission until SQLite has rolled back/closed and the
        // cancelled lease is retired. Aborting the request only signals it.
        tokio::spawn(async move {
            let _admission = admission;
            let mut result = store
                .run_annotation_preparation(&lease, id, &expected, Arc::clone(&cancelled), work)
                .await;
            if cancelled.load(Ordering::Relaxed) || result.is_err() {
                result =
                    preparation_cleanup_result(result, store.retire_annotation_lease(id).await);
            }
            result
        })
        .await
        .map_err(|error| intent_core::Error::Internal(format!("annotation worker: {error}")))?
    }

    async fn run_annotation_preparation(
        &self,
        lease: &Lease,
        id: Uuid,
        expected: &AnnotationEpochs,
        cancelled: Arc<AtomicBool>,
        work: Arc<AtomicUsize>,
    ) -> Result<()> {
        let snapshot = id.simple().to_string();
        // Detach: a cancelled future must never return a connection with a live
        // progress hook to the pool. Drop signals the worker before it closes.
        let mut conn = self
            .write_pool()
            .acquire()
            .await
            .map_err(db_error)?
            .detach();
        let result=async {
        sqlx::query("PRAGMA temp_store=FILE")
            .execute(&mut conn)
            .await
            .map_err(db_error)?;
        sqlx::query("PRAGMA cache_size=-2048")
            .execute(&mut conn)
            .await
            .map_err(db_error)?;
        install_preparation_budget(&mut conn, cancelled, Arc::clone(&work), lease.expires_ms)
            .await?;
            let mut tx=conn.begin_with("BEGIN IMMEDIATE").await.map_err(db_error)?;
            let ws=WorkspaceId::from(lease.scope.workspace_id.as_str());let note=NoteId::from(lease.scope.note_id.as_str());let (head_id,actual)=head(&mut tx,&ws,&note).await?;
            if actual.source_revision!=expected.source_revision||actual.comment_revision!=expected.comment_revision||!actual.anchors_ready{return Err(stale());}
            let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM note_annotation_snapshot WHERE id=? AND expires_ms>?)").bind(&snapshot).bind(i64::try_from(intent_core::now_epoch_ms()).map_err(|_|invalid())?).fetch_one(&mut *tx).await.map_err(db_error)?;
            if !exists{return Err(failure(NotePageError::Expired));}
            // Recheck under the writer lock; competing callers can share a lease.
            let done:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM note_annotation_match_head WHERE snapshot_id=?)").bind(&snapshot).fetch_one(&mut *tx).await.map_err(db_error)?;
            if done{tx.commit().await.map_err(db_error)?;return Ok(());}
            let ranges=lease.query.ranges.iter().map(|r|SourceRange{start:r.start,end:r.end}).collect::<Vec<_>>();
            let mut query=comments::matching_threads(head_id,&ranges,CommentFilter::Anchored);
            query.push("SELECT COUNT(*) AS threads,COALESCE(SUM(t.total_comments),0) AS comments FROM matched m CROSS JOIN note_comment_thread t ON t.head_id=").push_bind(head_id).push(" AND t.thread_id=m.thread_id");
            let totals=query.build().fetch_one(&mut *tx).await.map_err(|error| {
                if i64::try_from(intent_core::now_epoch_ms()).is_ok_and(|now| now>=lease.expires_ms) {failure(NotePageError::Expired)} else {db_error(error)}
            })?;
            sqlx::query("INSERT INTO note_annotation_match_head(snapshot_id,total_threads,total_comments,prepare_steps,head_id) VALUES(?,?,?,?,?)")
                .bind(&snapshot).bind(totals.get::<i64,_>("threads")).bind(totals.get::<i64,_>("comments")).bind(i64::try_from(work.load(Ordering::Relaxed)).map_err(|_|invalid())?).bind(head_id).execute(&mut *tx).await.map_err(db_error)?;
            if i64::try_from(intent_core::now_epoch_ms()).map_err(|_|invalid())? >= lease.expires_ms {
                return Err(failure(NotePageError::Expired));
            }
            tx.commit().await.map_err(db_error)
        }.await;
        let result = close_preparation_connection(conn, result).await;
        if i64::try_from(intent_core::now_epoch_ms()).map_err(|_| invalid())? >= lease.expires_ms {
            return match result {
                Ok(()) => Err(failure(NotePageError::Expired)),
                Err(error) => Err(error),
            };
        }
        result
    }

    pub(super) async fn read_annotation_matches(
        &self,
        lease: &Lease,
        id: Uuid,
        epochs: &AnnotationEpochs,
        after: Option<(i64, &str)>,
    ) -> Result<ThreadRows> {
        self.prepare_annotation_matches(lease, id, epochs).await?;
        self.validate_annotation_lease(id, lease, &lease.scope, &lease.principal)
            .await?;
        let mut tx = self.read_pool().begin().await.map_err(db_error)?;
        let ws = WorkspaceId::from(lease.scope.workspace_id.as_str());
        let note = NoteId::from(lease.scope.note_id.as_str());
        let (head_id, actual) = head(&mut tx, &ws, &note).await?;
        if actual.source_revision != epochs.source_revision
            || actual.comment_revision != epochs.comment_revision
        {
            return Err(stale());
        }
        let snapshot = id.simple().to_string();
        let totals=sqlx::query("SELECT total_threads,total_comments FROM note_annotation_match_head WHERE snapshot_id=?").bind(&snapshot).fetch_optional(&mut *tx).await.map_err(db_error)?.ok_or_else(||failure(NotePageError::Expired))?;
        let ranges = lease
            .query
            .ranges
            .iter()
            .map(|r| SourceRange {
                start: r.start,
                end: r.end,
            })
            .collect::<Vec<_>>();
        let mut rows = Vec::new();
        // Disjoint ranges order every later candidate after earlier ranges.
        // Stop when this page is full instead of examining every later range
        // (whose occurrences can all belong to already-emitted threads).
        for range_index in 0..ranges.len() {
            if after.is_some_and(|(position, _)| position >= ranges[range_index].end) {
                continue;
            }
            let mut query = match_summary_query(
                head_id,
                &ranges,
                range_index,
                after,
                i64::try_from(lease.query.items + 1 - rows.len()).map_err(|_| invalid())?,
            );
            rows.extend(query.build().fetch_all(&mut *tx).await.map_err(db_error)?);
            if rows.len() > lease.query.items {
                break;
            }
        }
        let has_more = rows.len() > lease.query.items;
        let items = rows
            .iter()
            .take(lease.query.items)
            .map(|r| ThreadRow {
                thread_id: r.get("thread_id"),
                status: r.get("status"),
                total_comments: r.get("total_comments"),
                root_comment_id: r.get("root_id"),
                root_present: r.get("root_present"),
                latest_comment_id: r.get("latest_id"),
                latest_comment_preview: r.get("preview"),
                truncated: r.get("truncated"),
                position: r.get("position"),
            })
            .collect();
        tx.commit().await.map_err(db_error)?;
        Ok(ThreadRows {
            page: AnnotationPage {
                epochs: actual,
                items,
                has_more,
            },
            total_threads: totals.get("total_threads"),
            total_comments: totals.get("total_comments"),
        })
    }
}

// Every exit after detach reaches this cleanup, including setup and progress
// handler failures. Keep both the primary and cleanup errors if both occur.
async fn close_preparation_connection(
    mut conn: sqlx::SqliteConnection,
    result: Result<()>,
) -> Result<()> {
    let remove = match conn.lock_handle().await {
        Ok(mut handle) => {
            handle.remove_progress_handler();
            Ok(())
        }
        Err(error) => Err(db_error(error)),
    };
    let close = conn.close().await.map_err(db_error);
    preparation_cleanup_result(preparation_cleanup_result(result, remove), close)
}

fn preparation_cleanup_result(primary: Result<()>, cleanup: Result<()>) -> Result<()> {
    match (primary, cleanup) {
        (result, Ok(())) => result,
        (Ok(()), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => Err(intent_core::Error::Internal(format!(
            "{primary}; annotation cleanup also failed: {cleanup}"
        ))),
    }
}

pub(in crate::note_annotation_repo) fn match_summary_query(
    head: i64,
    ranges: &[SourceRange],
    range_index: usize,
    after: Option<(i64, &str)>,
    take: i64,
) -> QueryBuilder<'static, sqlx::Sqlite> {
    let mut query = QueryBuilder::new("WITH ranges(start,end) AS (");
    if ranges.is_empty() {
        query.push("SELECT 0,0 WHERE 0");
    } else {
        query.push("VALUES ");
        for (i, range) in ranges.iter().enumerate() {
            if i > 0 {
                query.push(",");
            }
            query
                .push("(")
                .push_bind(range.start)
                .push(",")
                .push_bind(range.end)
                .push(")");
        }
    }
    query.push("), selected AS MATERIALIZED (SELECT thread_id,position FROM (");
    let mut first = true;
    for range in ranges.iter().skip(range_index).take(1) {
        if after.is_some_and(|(position, _)| position >= range.end) {
            continue;
        }
        if !first {
            query.push(" UNION ALL ");
        }
        first = false;
        query.push("SELECT thread_id,position FROM (SELECT a.thread_id,a.start AS position FROM note_comment_anchor a INDEXED BY note_comment_anchor_start_order WHERE a.head_id=").push_bind(head)
            .push(" AND a.start>=").push_bind(range.start).push(" AND a.start<").push_bind(range.end);
        if let Some((position, thread)) = after {
            query
                .push(" AND (a.start,a.thread_id)>(")
                .push_bind(position)
                .push(",")
                .push_bind(thread.to_owned())
                .push(")");
        }
        canonical(&mut query, "a.start");
        query
            .push(" ORDER BY a.start,a.thread_id,a.id LIMIT ")
            .push_bind(take)
            .push(")");
        if after.is_some_and(|(position, _)| position > range.start) {
            continue;
        }
        // A long interval crossing this range's start belongs to exactly one
        // of these point-path buckets. Each ordered stream admits at most take.
        query.push(" UNION ALL SELECT thread_id,position FROM (SELECT thread_id,position FROM (");
        for level in 0..=53 {
            if level > 0 {
                query.push(" UNION ALL ");
            }
            query.push("SELECT thread_id,position FROM (SELECT c.thread_id,").push_bind(range.start).push(" AS position FROM note_comment_anchor_cover c CROSS JOIN note_comment_anchor a ON a.id=c.anchor_id WHERE c.head_id=").push_bind(head)
                .push(" AND c.level=").push_bind(level).push(" AND c.bucket=").push_bind(range.start>>level)
                .push(" AND c.start<").push_bind(range.start);
            if let Some((position, thread)) = after {
                if position == range.start {
                    query.push(" AND c.thread_id>").push_bind(thread.to_owned());
                }
            }
            canonical(&mut query, &range.start.to_string());
            query
                .push(" ORDER BY c.thread_id,c.anchor_id LIMIT ")
                .push_bind(take)
                .push(")");
        }
        query
            .push(") ORDER BY position,thread_id LIMIT ")
            .push_bind(take)
            .push(")");
        // Wrap this union member so its ORDER/LIMIT cannot limit earlier ranges.
        // The outer SELECT is already the bounded selected CTE.
    }
    if first {
        query.push("SELECT '' AS thread_id,0 AS position WHERE 0");
    }
    query
        .push(") ORDER BY position,thread_id LIMIT ")
        .push_bind(take)
        .push(") ");
    comments::finish_thread_summary(query, head)
}

fn canonical(query: &mut QueryBuilder<'static, sqlx::Sqlite>, position: &str) {
    query.push(" AND NOT EXISTS (SELECT 1 FROM note_comment_anchor b INDEXED BY note_comment_anchor_thread_order JOIN ranges r ON (b.start<r.end AND b.end>r.start) OR (b.start=b.end AND b.start>=r.start AND b.start<r.end) WHERE b.head_id=a.head_id AND b.thread_id=a.thread_id AND (MAX(b.start,r.start),b.id)<(")
        .push(position.to_owned()).push(",a.id))");
}

#[cfg(test)]
mod tests;
