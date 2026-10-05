//! Requested anchored queries prepare narrow `SQLite` rows once per fixed lease.
//! First preparation is O(matching index entries), not O(page). No body/JSON or
//! whole ID collection enters Rust. Four leases retain at most four copies of
//! current scoped thread IDs; row cardinality never truncates or rejects results.
//! `SQLite` streams into disk storage; cancellation interrupts its worker query.
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

const MAX_PREPARED_QUERIES: i64 = 4;
static PREPARATION_SLOT: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);
struct CancelPreparation(Arc<AtomicBool>);
impl Drop for CancelPreparation {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

async fn install_preparation_budget(
    conn: &mut sqlx::SqliteConnection,
) -> Result<(CancelPreparation, Arc<AtomicUsize>)> {
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancel = CancelPreparation(Arc::clone(&cancelled));
    let work = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&work);
    conn.lock_handle()
        .await
        .map_err(db_error)?
        .set_progress_handler(1000, move || {
            counter.fetch_add(1000, Ordering::Relaxed);
            !cancelled.load(Ordering::Relaxed)
        });
    Ok((cancel, work))
}

impl Store {
    pub(super) async fn prepare_annotation_matches(
        &self,
        lease: &Lease,
        id: Uuid,
        expected: &AnnotationEpochs,
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
        let _admission = PREPARATION_SLOT.try_acquire().map_err(|_| budget())?;
        // Detach: a cancelled future must never return a connection with a live
        // progress hook to the pool. Drop signals the worker before it closes.
        let mut conn = self
            .write_pool()
            .acquire()
            .await
            .map_err(db_error)?
            .detach();
        sqlx::raw_sql("PRAGMA temp_store=FILE; PRAGMA cache_size=-2048;")
            .execute(&mut conn)
            .await
            .map_err(db_error)?;
        let (_cancel, work) = install_preparation_budget(&mut conn).await?;
        let result=async {
            let mut tx=conn.begin_with("BEGIN IMMEDIATE").await.map_err(db_error)?;
            let ws=WorkspaceId::from(lease.scope.workspace_id.as_str());let note=NoteId::from(lease.scope.note_id.as_str());let (head_id,actual)=head(&mut tx,&ws,&note).await?;
            if actual.source_revision!=expected.source_revision||actual.comment_revision!=expected.comment_revision||!actual.anchors_ready{return Err(stale());}
            let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM note_annotation_snapshot WHERE id=? AND expires_ms>?)").bind(&snapshot).bind(i64::try_from(intent_core::now_epoch_ms()).map_err(|_|invalid())?).fetch_one(&mut *tx).await.map_err(db_error)?;
            if !exists{return Err(failure(NotePageError::Expired));}
            // Recheck under the writer lock; competing callers can share a lease.
            let done:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM note_annotation_match_head WHERE snapshot_id=?)").bind(&snapshot).fetch_one(&mut *tx).await.map_err(db_error)?;
            if done{tx.commit().await.map_err(db_error)?;return Ok(());}
            let count:i64=sqlx::query_scalar("SELECT COUNT(*) FROM note_annotation_match_head").fetch_one(&mut *tx).await.map_err(db_error)?;
            if count>=MAX_PREPARED_QUERIES {
                sqlx::query("DELETE FROM note_annotation_snapshot WHERE id=(SELECT s.id FROM note_annotation_snapshot s JOIN note_annotation_match_head m ON m.snapshot_id=s.id ORDER BY s.expires_ms,s.id LIMIT 1)").execute(&mut *tx).await.map_err(db_error)?;
            }
            sqlx::query("INSERT INTO note_annotation_match_head VALUES(?,0,0,0,?)").bind(&snapshot).bind(head_id).execute(&mut *tx).await.map_err(db_error)?;
            let ranges=lease.query.ranges.iter().map(|r|SourceRange{start:r.start,end:r.end}).collect::<Vec<_>>();
            let mut query=comments::matching_threads(head_id,&ranges,CommentFilter::Anchored);
            query.push("INSERT INTO note_annotation_match(snapshot_id,thread_id,position) SELECT ").push_bind(snapshot.clone()).push(",thread_id,position FROM matched");
            query.build().execute(&mut *tx).await.map_err(|_|budget())?;
            let totals=sqlx::query("SELECT COUNT(*) AS threads,COALESCE(SUM(t.total_comments),0) AS comments FROM note_annotation_match m CROSS JOIN note_comment_thread t ON t.head_id=? AND t.thread_id=m.thread_id WHERE m.snapshot_id=?")
                .bind(head_id).bind(&snapshot).fetch_one(&mut *tx).await.map_err(db_error)?;
            let threads:i64=totals.get("threads");
            sqlx::query("UPDATE note_annotation_match_head SET total_threads=?,total_comments=?,prepare_steps=? WHERE snapshot_id=?").bind(threads).bind(totals.get::<i64,_>("comments")).bind(i64::try_from(work.load(Ordering::Relaxed)).map_err(|_|invalid())?).bind(&snapshot).execute(&mut *tx).await.map_err(db_error)?;
            tx.commit().await.map_err(db_error)
        }.await;
        conn.lock_handle()
            .await
            .map_err(db_error)?
            .remove_progress_handler();
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
        let mut query = match_summary_query(
            head_id,
            &snapshot,
            after,
            i64::try_from(lease.query.items + 1).map_err(|_| invalid())?,
        );
        let rows = query.build().fetch_all(&mut *tx).await.map_err(db_error)?;
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

pub(in crate::note_annotation_repo) fn match_summary_query(
    head: i64,
    snapshot: &str,
    after: Option<(i64, &str)>,
    take: i64,
) -> QueryBuilder<'static, sqlx::Sqlite> {
    let mut query=QueryBuilder::new("WITH selected AS MATERIALIZED (SELECT thread_id,position FROM note_annotation_match WHERE snapshot_id=");
    query.push_bind(snapshot.to_owned());
    if let Some((position, thread)) = after {
        query
            .push(" AND (position,thread_id)>(")
            .push_bind(position)
            .push(",")
            .push_bind(thread.to_owned())
            .push(")");
    }
    query
        .push(" ORDER BY position,thread_id LIMIT ")
        .push_bind(take)
        .push(") ");
    comments::finish_thread_summary(query, head)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn annotation_cancelled_preparation_interrupts_sql_and_retains_no_partial_rows() {
        let mut conn = sqlx::SqliteConnection::connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query("CREATE TABLE staged(id INTEGER)")
            .execute(&mut conn)
            .await
            .unwrap();
        let (guard, work) = install_preparation_budget(&mut conn).await.unwrap();
        drop(guard);
        let result=sqlx::query("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000) INSERT INTO staged SELECT x FROM n").execute(&mut conn).await;
        assert!(result.is_err());
        conn.lock_handle().await.unwrap().remove_progress_handler();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM staged")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(count, 0);
        assert!(work.load(Ordering::Relaxed) <= 1000);
    }
}
