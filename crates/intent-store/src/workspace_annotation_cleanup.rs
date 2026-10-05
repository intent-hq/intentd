//! Retire annotation children before the existing workspace note cascade.
//! A durable workspace fence rejects new annotation readers and rebuilding
//! writers between these separately committed, resumable statements.
use crate::Store;
use intent_core::{Error, Result};
use sqlx::{QueryBuilder, Sqlite};

pub(crate) const COMMENT_BATCH: i64 = 32;
const CHILD_BATCH: i64 = 500;

fn database_error(error: &sqlx::Error) -> Error {
    Error::Internal(format!("workspace annotation cleanup failed: {error}"))
}

/// Bounded parent clearing remains interleavable. The final absence check and
/// fence publication share the writer, so a racing parent adoption makes us
/// retry before retirement rather than trapping a link behind the fence.
pub(crate) async fn begin_retirement(store: &Store, workspace_id: &str) -> Result<()> {
    loop {
        let retired: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM note_annotation_workspace_retirement WHERE workspace_id=?)",
        )
        .bind(workspace_id)
        .fetch_one(store.read_pool())
        .await
        .map_err(|error| database_error(&error))?;
        if retired {
            return Ok(());
        }
        crate::agent_repo::delete_in_bounded_batches(
            store.write_pool(),
            crate::workspace_repo::CLEAR_NOTE_PARENT_BATCH_SQL,
            workspace_id,
            crate::agent_repo::DELETE_CASCADE_BATCH,
        )
        .await?;
        let mut tx = store
            .write_pool()
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|error| database_error(&error))?;
        let parent_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM note WHERE workspace_id=? AND parent_id IS NOT NULL)",
        )
        .bind(workspace_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|error| database_error(&error))?;
        if parent_exists {
            tx.rollback()
                .await
                .map_err(|error| database_error(&error))?;
            tokio::task::yield_now().await;
            continue;
        }
        // This marker is intentionally not removed by an error/drop guard.
        sqlx::query(
            "INSERT INTO note_annotation_workspace_retirement(workspace_id) \
             SELECT id FROM workspace WHERE id=? ON CONFLICT(workspace_id) DO NOTHING",
        )
        .bind(workspace_id)
        .execute(&mut *tx)
        .await
        .map_err(|error| database_error(&error))?;
        tx.commit().await.map_err(|error| database_error(&error))?;
        return Ok(());
    }
}

/// Each child query has an indexed owner prefix. Owners are limited to one
/// parent page; an empty prefix is never the entire already-processed workspace.
async fn drain_heads(
    store: &Store,
    table: &'static str,
    key: &'static str,
    heads: &[i64],
    batch: i64,
) -> Result<()> {
    let columns = key
        .strip_prefix('(')
        .and_then(|key| key.strip_suffix(')'))
        .unwrap_or(key);
    loop {
        let mut query = QueryBuilder::<Sqlite>::new(format!(
            "DELETE FROM {table} WHERE {key} IN (SELECT {columns} FROM {table} WHERE head_id IN ("
        ));
        let mut values = query.separated(",");
        for head in heads {
            values.push_bind(head);
        }
        query.push(") LIMIT ").push_bind(batch).push(")");
        let removed = query
            .build()
            .execute(store.write_pool())
            .await
            .map_err(|error| database_error(&error))?
            .rows_affected();
        tokio::task::yield_now().await;
        if removed < u64::try_from(batch).expect("positive cleanup batch") {
            return Ok(());
        }
    }
}

async fn drain_comment(store: &Store, comment_id: &str) -> Result<()> {
    crate::agent_repo::delete_in_bounded_batches(
        store.write_pool(),
        "DELETE FROM note_comment_detail_piece WHERE rowid IN \
         (SELECT rowid FROM note_comment_detail_piece WHERE comment_id=? LIMIT ?)",
        comment_id,
        CHILD_BATCH,
    )
    .await?;
    loop {
        let anchors: Vec<i64> =
            sqlx::query_scalar("SELECT id FROM note_comment_anchor WHERE comment_id=? LIMIT ?")
                .bind(comment_id)
                .bind(COMMENT_BATCH)
                // Keep candidate lookup on the same measured connection as
                // child and parent deletion; cost controls cover the full batch.
                .fetch_all(store.write_pool())
                .await
                .map_err(|error| database_error(&error))?;
        if anchors.is_empty() {
            return Ok(());
        }
        for anchor in &anchors {
            loop {
                // This table is WITHOUT ROWID; use its real composite key.
                let removed = sqlx::query(
                    "DELETE FROM note_comment_anchor_cover \
                     WHERE (head_id,level,bucket,thread_id,anchor_id) IN \
                     (SELECT head_id,level,bucket,thread_id,anchor_id \
                      FROM note_comment_anchor_cover WHERE anchor_id=? LIMIT ?)",
                )
                .bind(anchor)
                .bind(CHILD_BATCH)
                .execute(store.write_pool())
                .await
                .map_err(|error| database_error(&error))?
                .rows_affected();
                tokio::task::yield_now().await;
                if removed < u64::try_from(CHILD_BATCH).expect("positive cleanup batch") {
                    break;
                }
            }
        }
        let mut query =
            QueryBuilder::<Sqlite>::new("DELETE FROM note_comment_anchor WHERE id IN (");
        let mut values = query.separated(",");
        for anchor in &anchors {
            values.push_bind(anchor);
        }
        query.push(")");
        query
            .build()
            .execute(store.write_pool())
            .await
            .map_err(|error| database_error(&error))?;
        tokio::task::yield_now().await;
    }
}

pub(crate) async fn delete_comment_batch(store: &Store, workspace_id: &str) -> Result<u64> {
    let comments: Vec<String> = sqlx::query_scalar(
        "SELECT id FROM comment WHERE workspace_id=? AND note_id IS NOT NULL LIMIT ?",
    )
    .bind(workspace_id)
    .bind(COMMENT_BATCH)
    .fetch_all(store.write_pool())
    .await
    .map_err(|error| database_error(&error))?;
    if comments.is_empty() {
        return Ok(0);
    }
    for comment in &comments {
        drain_comment(store, comment).await?;
    }
    let mut query = QueryBuilder::<Sqlite>::new("DELETE FROM comment WHERE workspace_id=");
    query.push_bind(workspace_id).push(" AND id IN (");
    let mut values = query.separated(",");
    for comment in &comments {
        values.push_bind(comment);
    }
    query.push(")");
    let removed = query
        .build()
        .execute(store.write_pool())
        .await
        .map_err(|error| database_error(&error))?
        .rows_affected();
    tokio::task::yield_now().await;
    Ok(removed)
}

pub(crate) async fn drain(store: &Store, workspace_id: &str) -> Result<()> {
    begin_retirement(store, workspace_id).await?;
    while delete_comment_batch(store, workspace_id).await? > 0 {}

    let mut after: Option<String> = None;
    loop {
        let mut query = QueryBuilder::<Sqlite>::new(
            "SELECT id,note_id FROM note_annotation_head WHERE workspace_id=",
        );
        query.push_bind(workspace_id);
        if let Some(after) = &after {
            query.push(" AND note_id>").push_bind(after);
        }
        query
            .push(" ORDER BY note_id LIMIT ")
            .push_bind(COMMENT_BATCH);
        let owners: Vec<(i64, String)> = query
            .build_query_as()
            .fetch_all(store.read_pool())
            .await
            .map_err(|error| database_error(&error))?;
        if owners.is_empty() {
            return Ok(());
        }
        let heads: Vec<i64> = owners.iter().map(|(id, _)| *id).collect();
        for (table, key, batch) in [
            ("note_attribution_author_piece", "rowid", CHILD_BATCH),
            ("note_attribution_author", "rowid", CHILD_BATCH),
            ("note_attribution_line", "rowid", CHILD_BATCH),
            (
                "note_comment_anchor_cover",
                "(head_id,level,bucket,thread_id,anchor_id)",
                CHILD_BATCH,
            ),
            ("note_comment_anchor", "rowid", COMMENT_BATCH),
            ("note_comment_projection", "rowid", COMMENT_BATCH),
            ("note_comment_root", "rowid", CHILD_BATCH),
            ("note_comment_thread", "rowid", COMMENT_BATCH),
            ("note_annotation_match_head", "rowid", CHILD_BATCH),
        ] {
            drain_heads(store, table, key, &heads, batch).await?;
        }
        after = owners.last().map(|(_, note)| note.clone());
    }
}
